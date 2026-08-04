<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class PublicConnectionAwareInvalidationModel extends Model
{
    protected $connection = 'manual_reporting_alias';

    public $timestamps = false;

    protected $guarded = [];

    public function getTable(): string
    {
        return $this->getConnectionName() === 'manual_primary_alias'
            ? 'primary_users'
            : 'reporting_users';
    }
}

final class PublicInvalidationTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Public',
            'views' => 1,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_flush_all_is_one_epoch_increment_and_invalidates_every_payload(): void
    {
        $read = fn() => DB::table('posts')->where('id', $this->postId)->first();
        $read();
        $read();

        $before = (int) ($this->cacheStore()->getRaw(
            $this->cacheKeys()->epoch(),
        ) ?? '0');

        $this->assertTrue(NormCache::flushAll());
        $this->assertSame(
            $before + 1,
            (int) $this->cacheStore()->getRaw($this->cacheKeys()->epoch()),
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_flush_tag_invalidates_only_that_query_namespace(): void
    {
        $tagged = fn() => DB::table('posts')->where('id', $this->postId)->tag('homepage')->get();
        $untagged = fn() => DB::table('posts')->where('id', $this->postId)->get();

        $tagged();
        $untagged();
        $tagged();
        $untagged();

        $this->assertTrue(NormCache::flushTag('homepage'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $tagged();
        $untagged();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_table_invalidation_is_broad_and_boolean(): void
    {
        DB::table('posts')->get();
        $this->assertTrue(NormCache::invalidateTable('testing', 'posts'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        DB::table('posts')->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_invalidate_accepts_table_names_models_and_model_classes(): void
    {
        Post::query()->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);
        $versionKey = $this->cacheKeys()->version($identity);
        $generationKey = $this->cacheKeys()->generation($identity);
        $version = (int) ($this->cacheStore()->getRaw($versionKey) ?? '0');
        $generation = (int) ($this->cacheStore()->getRaw($generationKey) ?? '0');

        $this->assertTrue(NormCache::invalidate([
            'posts',
            Post::class,
            new Post,
        ]));
        $this->assertSame($version + 1, (int) $this->cacheStore()->getRaw($versionKey));
        $this->assertSame($generation + 1, (int) $this->cacheStore()->getRaw($generationKey));

        DB::flushQueryLog();
        DB::enableQueryLog();
        Post::query()->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    /**
     * @return list<array{0: callable(): (Model|string)}>
     */
    public static function scalarInvalidationTargets(): array
    {
        return [
            'table name' => [fn() => 'posts'],
            'model class' => [fn() => Post::class],
            'model instance' => [fn() => new Post],
            'hydrated model' => [fn() => Post::query()->firstOrFail()],
        ];
    }

    #[DataProvider('scalarInvalidationTargets')]
    public function test_invalidate_accepts_a_single_unwrapped_target(callable $target): void
    {
        Post::query()->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);
        $versionKey = $this->cacheKeys()->version($identity);
        $generationKey = $this->cacheKeys()->generation($identity);
        $version = (int) ($this->cacheStore()->getRaw($versionKey) ?? '0');
        $generation = (int) ($this->cacheStore()->getRaw($generationKey) ?? '0');

        $this->assertTrue(NormCache::invalidate($target()));
        $this->assertSame($version + 1, (int) $this->cacheStore()->getRaw($versionKey));
        $this->assertSame($generation + 1, (int) $this->cacheStore()->getRaw($generationKey));

        DB::flushQueryLog();
        DB::enableQueryLog();
        Post::query()->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_clearing_schema_alone_keeps_payloads_reachable(): void
    {
        $read = $this->readReshapedTable();

        $read();
        $read();
        DB::statement('alter table reshaped add column subtitle text');
        $this->assertTrue(NormCache::clearSchema());

        $this->assertArrayNotHasKey('subtitle', $read());
    }

    public function test_refreshing_schema_retires_payloads_shaped_by_the_old_schema(): void
    {
        $read = $this->readReshapedTable();

        $read();
        $read();
        DB::statement('alter table reshaped add column subtitle text');

        $this->assertTrue(NormCache::refreshSchema('testing'));
        $this->assertArrayHasKey('subtitle', $read());
    }

    public function test_refreshing_schema_retires_a_dropped_column(): void
    {
        $read = $this->readReshapedTable();

        $read();
        $read();
        DB::statement('alter table reshaped drop column removable');

        $this->assertTrue(NormCache::refreshSchema('testing'));
        $this->assertArrayNotHasKey('removable', $read());
    }

    public function test_refreshing_one_alias_retires_shared_source_schema_metadata_on_another_alias(): void
    {
        $database = tempnam(sys_get_temp_dir(), 'normcache-schema-alias-');
        $this->assertIsString($database);

        $readAlias = 'schema_read_alias';
        $writeAlias = 'schema_write_alias';
        $scope = 'shared-schema-source';
        $this->configureSqliteConnection($readAlias, $database, $scope);
        $this->configureSqliteConnection($writeAlias, $database, $scope);

        try {
            $read = DB::connection($readAlias);
            $write = DB::connection($writeAlias);
            $pdo = $write->getPdo();
            $pdo->exec('create table posts (id integer primary key, title text not null)');
            $pdo->exec('create table alias_subject (id integer primary key, title text not null)');
            $pdo->exec("insert into posts (id, title) values (1, 'Before')");
            $pdo->exec("insert into alias_subject (id, title) values (1, 'Before')");

            $readSubject = static fn() => $read
                ->table('alias_subject')
                ->where('id', 1)
                ->value('title');

            $this->assertSame('Before', $readSubject());
            $this->assertSame('Before', $readSubject());

            $pdo->exec('drop table alias_subject');
            $pdo->exec('create view alias_subject as select id, title from posts');

            $this->assertTrue(NormCache::refreshSchema($writeAlias));
            $this->app->forgetScopedInstances();

            $this->assertSame('Before', $readSubject());
            $this->assertSame('Before', $readSubject());

            $pdo->exec("update posts set title = 'After' where id = 1");

            $this->assertSame('After', $readSubject());
        } finally {
            DB::disconnect($readAlias);
            DB::disconnect($writeAlias);
            DB::purge($readAlias);
            DB::purge($writeAlias);

            if (is_file($database)) {
                unlink($database);
            }
        }
    }

    public function test_model_invalidation_applies_the_connection_before_resolving_the_table(): void
    {
        $database = tempnam(sys_get_temp_dir(), 'normcache-manual-invalidation-');
        $this->assertIsString($database);

        $connectionName = 'manual_primary_alias';
        $this->configureSqliteConnection($connectionName, $database, $connectionName);

        try {
            $connection = DB::connection($connectionName);
            $pdo = $connection->getPdo();
            $pdo->exec('create table primary_users (id integer primary key, name text not null)');
            $pdo->exec('create table reporting_users (id integer primary key, name text not null)');
            $pdo->exec("insert into primary_users (id, name) values (1, 'Before')");

            $readPrimary = static fn() => $connection
                ->table('primary_users')
                ->where('id', 1)
                ->value('name');

            $this->assertSame('Before', $readPrimary());
            $this->assertSame('Before', $readPrimary());

            $pdo->exec("update primary_users set name = 'After class' where id = 1");

            $this->assertTrue(NormCache::invalidate(
                PublicConnectionAwareInvalidationModel::class,
                $connectionName,
            ));
            $this->assertSame('After class', $readPrimary());
            $this->assertSame('After class', $readPrimary());

            $pdo->exec("update primary_users set name = 'After instance' where id = 1");
            $model = new PublicConnectionAwareInvalidationModel;

            $this->assertSame('manual_reporting_alias', $model->getConnectionName());
            $this->assertTrue(NormCache::invalidate($model, $connectionName));
            $this->assertSame('manual_reporting_alias', $model->getConnectionName());
            $this->assertSame('After instance', $readPrimary());
        } finally {
            DB::disconnect($connectionName);
            DB::purge($connectionName);

            if (is_file($database)) {
                unlink($database);
            }
        }
    }

    private function readReshapedTable(): \Closure
    {
        DB::statement('drop table if exists reshaped');
        DB::statement('create table reshaped (id integer primary key, title text, removable text)');
        DB::table('reshaped')->insert(['id' => 1, 'title' => 'Row', 'removable' => 'x']);

        return static fn(): array => (array) DB::table('reshaped')->where('id', 1)->first();
    }

    private function configureSqliteConnection(
        string $name,
        string $database,
        string $scope,
    ): void {
        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
            'normcache_scope' => $scope,
        ]);
        DB::purge($name);
    }
}
