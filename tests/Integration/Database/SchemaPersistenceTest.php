<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Builder;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use Mockery;
use NormCache\Database\QueryBuilder;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

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

        $this->assertTrue($warmRepository->clear('testing'));

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

    public function test_clearing_every_connection_retires_persisted_metadata(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $repository = $this->repository();
        $repository->putEffectiveSchema($connection, 'public');

        $this->assertSame('public', $this->repository()->effectiveSchema($connection));
        $this->assertTrue($repository->clear());
        $this->assertFalse($this->repository()->effectiveSchema($connection));
    }

    public function test_connection_clear_keeps_in_flight_writers_in_a_retired_generation(): void
    {
        $connection = $this->mysqlConnection(Mockery::mock(Builder::class));
        $inFlight = $this->repository();
        $inFlight->putEffectiveSchema($connection, 'before_clear');

        $this->assertTrue($this->repository()->clear('testing'));

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

                        if ($method === 'get') {
                            return str_contains((string) ($arguments[0] ?? ''), '{ncm}:schema-epoch')
                                ? '1'
                                : null;
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
