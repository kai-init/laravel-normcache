<?php

namespace NormCache\Tests\Unit;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Database\QueryBuilder;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\UnitTestCase;
use NormCache\Traits\Cacheable;

final class ReportingUser extends Model
{
    use Cacheable;

    protected $connection = 'reporting';

    protected $table = 'users';
}

final class AmbientUser extends Model
{
    use Cacheable;

    protected $table = 'users';
}

final class ConnectionAwareTableUser extends Model
{
    use Cacheable;

    protected $connection = 'reporting';

    public function getTable()
    {
        return $this->getConnectionName() === 'primary'
            ? 'primary_users'
            : 'reporting_users';
    }
}

final class DependencyAnalyzerTest extends UnitTestCase
{
    private string $primaryDatabase = '';

    private string $reportingDatabase = '';

    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $this->primaryDatabase = (string) tempnam(sys_get_temp_dir(), 'nc_primary_');
        $this->reportingDatabase = (string) tempnam(sys_get_temp_dir(), 'nc_reporting_');

        foreach (
            ['primary' => $this->primaryDatabase, 'reporting' => $this->reportingDatabase] as $name => $database
        ) {
            $app['config']->set('database.connections.' . $name, [
                'driver' => 'sqlite',
                'database' => $database,
                'prefix' => '',
            ]);
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        foreach ([$this->primaryDatabase, $this->reportingDatabase] as $database) {
            if ($database !== '' && file_exists($database)) {
                unlink($database);
            }
        }
    }

    public function test_declared_model_dependency_uses_the_active_query_connection(): void
    {
        $this->createUsersTables();

        $resolver = app(TableIdentityResolver::class);
        $primary = $resolver->resolve(DB::connection('primary'), 'users');
        $reporting = $resolver->resolve(DB::connection('reporting'), 'users');

        $this->assertNotSame(
            $primary?->hash,
            $reporting?->hash,
            'the two connections must resolve to distinct identities for this test to mean anything',
        );

        $query = ReportingUser::on('primary')->toBase()
            ->from('users')
            ->dependsOn([ReportingUser::class]);
        $this->assertInstanceOf(QueryBuilder::class, $query);
        $analysis = app(DependencyAnalyzer::class)
            ->analyze(DB::connection('primary'), $query);

        $this->assertSame($primary?->hash, $analysis->root?->hash);
        $this->assertSame(
            [$primary?->hash],
            array_map(static fn($table): string => $table->hash, $analysis->tables),
        );
    }

    public function test_declared_model_dependency_without_a_connection_uses_the_querying_connection(): void
    {
        $this->createUsersTables();

        $reporting = app(TableIdentityResolver::class)
            ->resolve(DB::connection('reporting'), 'users');

        $query = AmbientUser::on('reporting')->toBase()
            ->from('users')
            ->dependsOn([AmbientUser::class]);
        $this->assertInstanceOf(QueryBuilder::class, $query);
        $analysis = app(DependencyAnalyzer::class)
            ->analyze(DB::connection('reporting'), $query);

        $this->assertSame($reporting?->hash, $analysis->root?->hash);
    }

    public function test_cross_database_eloquent_subquery_captures_laravel_normalized_builder(): void
    {
        $outer = AmbientUser::on('primary')->toBase()->from('users');
        $this->assertInstanceOf(QueryBuilder::class, $outer);

        $outer->selectSub(ReportingUser::query(), 'reporting_user');
        $expression = end($outer->columns);
        $this->assertInstanceOf(Expression::class, $expression);

        $captured = $outer->capturedSubquery($expression);
        $this->assertNotNull($captured);
        $capturedSql = $captured->getGrammar()->compileSelect($captured);
        $expressionSql = (string) $expression->getValue($outer->getGrammar());

        $this->assertStringContainsString($capturedSql, $expressionSql);
        $this->assertNotSame('users', $captured->from);
    }

    public function test_declared_model_observes_the_active_connection_when_resolving_its_table(): void
    {
        Schema::connection('primary')->create('primary_users', function ($table): void {
            $table->id();
        });
        Schema::connection('reporting')->create('reporting_users', function ($table): void {
            $table->id();
        });

        $resolver = app(TableIdentityResolver::class);
        $primary = $resolver->resolve(DB::connection('primary'), 'primary_users');
        $query = ConnectionAwareTableUser::on('primary')->toBase()
            ->fromRaw('(select 1) as derived')
            ->dependsOn([ConnectionAwareTableUser::class]);
        $this->assertInstanceOf(QueryBuilder::class, $query);
        $analysis = app(DependencyAnalyzer::class)
            ->analyze(DB::connection('primary'), $query);

        $this->assertSame($primary?->hash, $analysis->root?->hash);
    }

    public function test_raw_source_without_a_model_is_unidentifiable_rather_than_fatal(): void
    {
        $this->createUsersTables();

        $derived = AmbientUser::on('primary')->toBase()->newQuery();
        $this->assertInstanceOf(QueryBuilder::class, $derived);
        $this->assertNull($derived->modelClass());

        $derived->fromRaw('(select 1) as derived');
        $analysis = app(DependencyAnalyzer::class)
            ->analyze(DB::connection('primary'), $derived);

        $this->assertNull($analysis->root);
        $this->assertSame('unidentifiable_dependency', $analysis->bypassReason);
    }

    private function createUsersTables(): void
    {
        foreach (['primary', 'reporting'] as $connection) {
            Schema::connection($connection)->create('users', function ($table): void {
                $table->id();
                $table->string('name')->nullable();
            });
        }
    }
}
