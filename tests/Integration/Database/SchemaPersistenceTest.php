<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Builder;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use Mockery;
use NormCache\Database\QueryBuilder;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use NormCache\Values\CacheConfig;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

final class MisconfiguredSchemaPost extends Post
{
    protected $table = 'posts';

    protected $primaryKey = 'missing_id';
}

final class CompositeAuthorTag extends Model
{
    use Cacheable;

    protected $table = 'author_tag';

    public $timestamps = false;

    protected $guarded = [];
}

final class SchemaPersistenceTest extends TestCase
{
    public function test_schema_is_reused_by_fresh_resolver_instances_and_can_be_cleared(): void
    {
        $firstBuilder = $this->schemaBuilder(expectIntrospection: true);
        $firstConnection = $this->mysqlConnection($firstBuilder);
        $firstRepository = $this->repository();
        $firstTable = (new TableIdentityResolver($firstRepository))
            ->resolve($firstConnection, 'posts');

        $this->assertNotNull($firstTable);

        $firstPrimaryKey = (new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $firstRepository,
        ))->resolve($this->normCacheQuery(), $firstConnection, $firstTable);

        $this->assertSame('id', $firstPrimaryKey?->column);
        $this->assertSame('integer', $firstPrimaryKey?->family);

        $warmBuilder = $this->schemaBuilder(expectIntrospection: false);
        $warmConnection = $this->mysqlConnection($warmBuilder);
        $warmRepository = $this->repository();
        $warmTable = (new TableIdentityResolver($warmRepository))
            ->resolve($warmConnection, 'posts');

        $this->assertNotNull($warmTable);

        $warmPrimaryKey = (new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $warmRepository,
        ))->resolve($this->normCacheQuery(), $warmConnection, $warmTable);

        $this->assertSame('id', $warmPrimaryKey?->column);
        $this->assertSame('integer', $warmPrimaryKey?->family);

        $this->assertTrue($warmRepository->clear());

        $coldBuilder = $this->schemaBuilder(expectIntrospection: true);
        $coldConnection = $this->mysqlConnection($coldBuilder);
        $coldRepository = $this->repository();
        $coldTable = (new TableIdentityResolver($coldRepository))
            ->resolve($coldConnection, 'posts');

        $this->assertNotNull($coldTable);

        $coldPrimaryKey = (new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $coldRepository,
        ))->resolve($this->normCacheQuery(), $coldConnection, $coldTable);

        $this->assertSame('id', $coldPrimaryKey?->column);
    }

    public function test_fresh_sqlite_schema_resolution_reads_one_epoch_and_batches_metadata_fields(): void
    {
        $connection = DB::connection();
        $query = DB::table('posts');
        $warmRepository = $this->repository();
        $table = (new TableIdentityResolver($warmRepository))->resolve($connection, 'posts');

        $this->assertNotNull($table);
        $this->assertNotNull((new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $warmRepository,
        ))->resolve($query, $connection, $table));

        $commands = [];
        $redis = Redis::connection('normcache-test');
        $redis->setEventDispatcher($this->app->make('events'));
        $redis->listen(static function (CommandExecuted $event) use (&$commands): void {
            $commands[] = strtolower((string) $event->command);
        });

        $freshRepository = $this->repository();
        $freshTable = (new TableIdentityResolver($freshRepository))->resolve($connection, 'posts');

        $this->assertNotNull($freshTable);
        $this->assertNotNull((new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $freshRepository,
        ))->resolve($query, $connection, $freshTable));

        $metadataCommands = array_values(array_filter(
            $commands,
            static fn(string $command): bool => in_array(
                $command,
                ['get', 'mget', 'hget', 'hmget'],
                true,
            ),
        ));

        $this->assertSame(['get', 'hmget'], $metadataCommands);
    }

    public function test_verified_schema_rejects_model_primary_key_assumptions(): void
    {
        $connection = DB::connection();
        $resolver = $this->app->make(PrimaryKeyResolver::class);
        $tables = $this->app->make(TableIdentityResolver::class);
        $posts = $tables->resolve($connection, 'posts');
        $pivot = $tables->resolve($connection, 'author_tag');

        $this->assertNotNull($posts);
        $this->assertNotNull($pivot);
        $this->assertSame(
            'missing_id',
            MisconfiguredSchemaPost::query()->toBase()->primaryKey()?->column,
        );
        $this->assertNull($resolver->resolve(
            MisconfiguredSchemaPost::query()->toBase(),
            $connection,
            $posts,
        ));
        $this->assertSame('id', CompositeAuthorTag::query()->toBase()->primaryKey()?->column);
        $this->assertNull($resolver->resolve(
            CompositeAuthorTag::query()->toBase(),
            $connection,
            $pivot,
        ));
    }

    public function test_explicit_primary_key_override_can_authorize_an_unsupported_schema_key(): void
    {
        $original = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['primary_keys'] = [[
            'connection' => 'testing',
            'database' => (string) realpath((string) DB::connection()->getDatabaseName()),
            'tables' => [
                'author_tag' => ['column' => 'author_id', 'type' => 'integer'],
            ],
        ]];
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        try {
            $connection = DB::connection();
            $query = DB::table('author_tag');
            $table = $this->app->make(TableIdentityResolver::class)
                ->resolve($connection, 'author_tag');

            $this->assertNotNull($table);
            $metadata = $this->app->make(PrimaryKeyResolver::class)->resolve(
                $query,
                $connection,
                $table,
            );

            $this->assertNotNull($metadata);
            $this->assertSame('author_id', $metadata->column);
            $this->assertSame('integer', $metadata->family);
        } finally {
            $this->app->instance(CacheConfig::class, $original);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_clearing_every_connection_retires_persisted_metadata(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $repository = $this->repository();
        $repository->putEffectiveSchema($connection, 'public');

        $this->assertSame('public', $this->repository()->effectiveSchema($connection));
        $this->assertTrue($repository->clear());
        $this->assertFalse($this->repository()->effectiveSchema($connection));
    }

    public function test_global_clear_keeps_in_flight_writers_in_a_retired_generation(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $inFlight = $this->repository();
        $inFlight->putEffectiveSchema($connection, 'before_clear');

        $this->assertTrue($this->repository()->clear());

        $inFlight->putEffectiveSchema($connection, 'stale_write');

        $this->assertFalse($this->repository()->effectiveSchema($connection));
    }

    public function test_failed_epoch_reads_do_not_fall_back_to_a_retired_generation(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $redis = $this->app->make('redis');

        try {
            $manager = $this->recoveringRedisManager();
            $this->app->instance('redis', $manager);
            Redis::clearResolvedInstance('redis');

            $recovering = new SchemaRepository(
                $this->app->make(CacheConfig::class),
                new RedisStore('normcache-test'),
                $this->app->make(CacheKeyBuilder::class),
            );

            $this->assertFalse($recovering->effectiveSchema($connection));
            $this->assertSame('current', $recovering->effectiveSchema($connection));
            $this->assertSame(3, $manager->built);
        } finally {
            $this->app->instance('redis', $redis);
            Redis::clearResolvedInstance('redis');
        }
    }

    public function test_a_zero_ttl_disables_persistence_entirely(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $repository = new SchemaRepository(
            CacheConfig::fromArray([...config('normcache'), 'schema_ttl' => 0]),
            $this->app->make(RedisStore::class),
            $this->app->make(CacheKeyBuilder::class),
        );

        $repository->putEffectiveSchema($connection, 'public');
        $repository->putPrimaryKey($this->table(), null);

        $this->assertFalse($repository->effectiveSchema($connection));
        $this->assertSame([], $this->cacheKeysMatching(':schema:v'));
    }

    public function test_expiry_is_claimed_once_rather_than_slid_by_every_write(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $repository = $this->repository();
        $repository->putEffectiveSchema($connection, 'public');

        $keys = $this->cacheKeysMatching(':schema:v');
        $this->assertCount(1, $keys);

        // A sliding expiry would push this back to the configured ttl and let a
        // busy application keep retired metadata alive forever.
        $redis = Redis::connection('normcache-test');
        $redis->expire($keys[0], 5);
        $repository->putViews($connection, 'public', ['post_titles' => true]);

        $this->assertLessThanOrEqual(5, $redis->ttl($keys[0]));
    }

    private function table(): TableIdentity
    {
        return TableIdentity::fromParts('mysql', 'testing', 'app', '', '', 'posts');
    }

    private function schemaBuilder(bool $expectIntrospection): Builder
    {
        $builder = Mockery::mock(Builder::class);

        if (!$expectIntrospection) {
            $builder->shouldNotReceive('getViews');
            $builder->shouldNotReceive('getIndexes');
            $builder->shouldNotReceive('getColumns');

            return $builder;
        }

        $builder->shouldReceive('getViews')->once()->with('app')->andReturn([]);
        $builder->shouldReceive('getIndexes')->once()->with('app.posts')->andReturn([[
            'name' => 'primary',
            'columns' => ['id'],
            'primary' => true,
        ]]);
        $builder->shouldReceive('getColumns')->once()->with('app.posts')->andReturn([[
            'name' => 'id',
            'type_name' => 'bigint',
        ]]);

        return $builder;
    }

    private function mysqlConnection(Builder $builder): Connection
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getDriverName')->andReturn('mysql');
        $connection->shouldReceive('getName')->andReturn('testing');
        $connection->shouldReceive('getDatabaseName')->andReturn('app');
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')
            ->andReturn(['name' => 'testing']);
        $connection->shouldReceive('getSchemaBuilder')->andReturn($builder);

        return $connection;
    }

    private function repository(): SchemaRepository
    {
        return new SchemaRepository(
            $this->app->make(CacheConfig::class),
            $this->app->make(RedisStore::class),
            $this->app->make(CacheKeyBuilder::class),
        );
    }

    private function recoveringRedisManager(): object
    {
        return new class
        {
            public int $built = 0;

            public function connection($name = null): RedisConnection
            {
                $attempt = $this->built++;
                $client = new class($attempt)
                {
                    public function __construct(private readonly int $attempt) {}

                    /** @param list<mixed> $arguments */
                    public function __call(string $method, array $arguments): mixed
                    {
                        if ($this->attempt < 2) {
                            throw new \RuntimeException('Redis unavailable');
                        }

                        if ($method === 'mget') {
                            $keys = (array) ($arguments[0] ?? []);

                            return array_map(
                                static fn(string $key): ?string => str_contains($key, '{ncm}:schema-epoch')
                                    ? '1'
                                    : null,
                                $keys,
                            );
                        }

                        if ($method === 'hget') {
                            return str_contains((string) ($arguments[0] ?? ''), ':schema:v0:')
                                ? 'retired'
                                : 'current';
                        }

                        return null;
                    }
                };

                return new class($client) extends RedisConnection
                {
                    public function __construct(mixed $client)
                    {
                        $this->client = $client;
                    }

                    public function createSubscription($channels, \Closure $callback, $method = 'subscribe'): void {}
                };
            }

            public function purge(string $name): void {}
        };
    }

    private function normCacheQuery(): QueryBuilder
    {
        return new QueryBuilder(DB::connection());
    }
}
