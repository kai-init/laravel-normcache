<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\UnitTestCase;

class ReportingUser extends Model
{
    protected $connection = 'reporting';

    protected $table = 'users';
}

class AmbientUser extends Model
{
    protected $table = 'users';
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

    public function test_declared_model_dependency_resolves_on_the_models_own_connection(): void
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

        $declared = app(DependencyAnalyzer::class)
            ->modelIdentity(DB::connection('primary'), ReportingUser::class);

        $this->assertSame($reporting?->hash, $declared?->hash);
    }

    public function test_declared_model_dependency_without_a_connection_uses_the_querying_connection(): void
    {
        $this->createUsersTables();

        $reporting = app(TableIdentityResolver::class)
            ->resolve(DB::connection('reporting'), 'users');

        $declared = app(DependencyAnalyzer::class)
            ->modelIdentity(DB::connection('reporting'), AmbientUser::class);

        $this->assertSame($reporting?->hash, $declared?->hash);
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
