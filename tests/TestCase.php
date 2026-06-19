<?php

namespace NormCache\Tests;

use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Pagination\CursorPaginator;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\ExecutionEngine;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\NormalizedCacheReader;
use NormCache\Cache\NormalizedThroughReader;
use NormCache\Cache\ResultCacheReader;
use NormCache\Cache\ResultExecutor;
use NormCache\Cache\VersionTracker;
use NormCache\CacheManager;
use NormCache\CacheServiceProvider;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use Orchestra\Testbench\TestCase as OrchestraTestCase;
use Predis\Client;
use ReflectionProperty;

abstract class TestCase extends OrchestraTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $redis = Redis::connection('normcache-test');
        $client = $redis->client();

        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            if (class_exists(Client::class) && $client instanceof Client) {
                foreach ($client as $node) {
                    try {
                        $node->flushdb();
                    } catch (\Exception $e) {
                        // Ignore READONLY errors from replicas
                    }
                }
            } elseif ($client instanceof \RedisCluster) {
                foreach ($client->_masters() as $master) {
                    $client->flushdb($master);
                }
            }
        } else {
            $redis->flushdb();
        }

        $this->resetClassKeyCache();
    }

    protected function getPackageProviders($app): array
    {
        return [CacheServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);

        $client = env('REDIS_CLIENT', 'phpredis');
        $app['config']->set('database.redis.client', $client);
        $app['config']->set('database.redis.options.prefix', '');

        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            if ($client === 'predis') {
                $app['config']->set('database.redis.options.cluster', 'redis');
            }

            $nodes = explode(',', env('REDIS_CLUSTER_NODES', '127.0.0.1:6379'));
            $app['config']->set('database.redis.clusters.normcache-test', array_map(function ($node) {
                [$host, $port] = explode(':', $node);

                return [
                    'host' => $host,
                    'port' => $port,
                    'database' => 0,
                    'password' => env('REDIS_PASSWORD', null),
                ];
            }, $nodes));
            $app['config']->set('normcache.cluster', true);
        } else {
            $app['config']->set('database.redis.normcache-test', [
                'host' => env('REDIS_HOST', '127.0.0.1'),
                'port' => env('REDIS_PORT', 6379),
                'database' => 15,
                'password' => env('REDIS_PASSWORD', null),
            ]);
        }

        $app['config']->set('normcache.connection', 'normcache-test');
        $app['config']->set('normcache.enabled', true);
        $app['config']->set('normcache.events', true);
        $app['config']->set('normcache.key_prefix', 'test:');
        $app['config']->set('normcache.slotting', true);
        $app['config']->set('normcache.ttl', 3600);
        $app['config']->set('normcache.query_ttl', 60);
        $app['config']->set('normcache.cooldown', 0);
    }

    protected function defineDatabaseMigrations(): void
    {
        $this->loadMigrationsFrom(__DIR__ . '/Fixtures/database');
    }

    protected function resetClassKeyCache(): void
    {
        (new ReflectionProperty(CacheKeyBuilder::class, 'classKeys'))->setValue(null, []);
        (new ReflectionProperty(CacheKeyBuilder::class, 'prototypes'))->setValue(null, []);
        (new ReflectionProperty(CacheKeyBuilder::class, 'deletedAtColumns'))->setValue(null, []);
    }

    protected function modelCacheEntry(string $class, mixed $id): mixed
    {
        $key = 'model:{' . $this->cacheManager()->classKey($class) . '}:' . $id;

        return $this->cacheManager()->getStore()->get($key);
    }

    protected function evictModelCache(string $class, mixed $id): void
    {
        $key = 'model:{' . $this->cacheManager()->classKey($class) . '}:' . $id;
        $this->cacheManager()->getStore()->delete($key);
    }

    protected function prefixedModelKey(string $class, mixed $id): string
    {
        $key = 'model:{' . $this->cacheManager()->classKey($class) . '}:' . $id;

        return $this->cacheManager()->getStore()->prefix($key);
    }

    protected function redisKeys(string $pattern = '*'): array
    {
        return $this->cacheManager()->getStore()->scanPattern($pattern);
    }

    protected function cacheManager(): CacheManager
    {
        return $this->app->make('normcache');
    }

    /**
     * Build a standalone CacheManager (not bound in the container) for tests
     * that need specific construction parameters like cooldown or slotting.
     */
    protected function buildManager(
        string $connection = 'normcache-test',
        ?int $ttl = null,
        ?int $queryTtl = null,
        string $keyPrefix = 'test:',
        int $cooldown = 0,
        bool $cluster = false,
        bool $enabled = true,
        bool $dispatchEvents = true,
        bool $fallback = false,
        bool $fireRetrieved = false,
        int $buildingLockTtl = 5,
        int $stampedeWaitMs = 200,
        int $staleDepth = 3,
        bool $slotting = false,
    ): CacheManager {
        $ttl ??= (int) config('normcache.ttl');
        $queryTtl ??= (int) config('normcache.query_ttl');

        $slottingActive = $cluster && $slotting;
        $store = new RedisStore($connection, $keyPrefix, $slottingActive, $slotting ? '' : '{nc}:');
        $keys = new CacheKeyBuilder;
        $versions = new VersionTracker($store, $keys);
        $result = new ResultExecutor;

        return new CacheManager(
            queryReader: new NormalizedCacheReader($store, $keys, $versions, $queryTtl, $buildingLockTtl, $staleDepth, $stampedeWaitMs),
            resultReader: new ResultCacheReader($store, $keys, $versions, $queryTtl, $buildingLockTtl, $stampedeWaitMs, $slottingActive),
            throughReader: new NormalizedThroughReader($store, $keys, $versions, $queryTtl, $buildingLockTtl, $stampedeWaitMs),
            result: $result,
            hydrator: new ModelHydrator($store, $keys, $versions, $ttl, $fireRetrieved, $buildingLockTtl, $stampedeWaitMs),
            versions: $versions,
            engine: new ExecutionEngine,
            store: $store,
            keys: $keys,
            config: new CacheConfig(
                ttl: $ttl,
                queryTtl: $queryTtl,
                cooldown: $cooldown,
                enabled: $enabled,
                fallbackEnabled: $fallback,
                dispatchEvents: $dispatchEvents,
                cluster: $cluster,
                slotting: $slottingActive,
            ),
        );
    }

    /** Assert native == cold == warm for a given query. */
    protected function contract(callable $cached, callable $native): void
    {
        $expected = $this->normalize($native());
        $cold = $this->normalize($cached());

        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            $warm = $this->normalize($cached());
            $warmQueries = DB::getQueryLog();
        } finally {
            DB::disableQueryLog();
        }

        if ($warmQueries !== []) {
            fwrite(STDERR, sprintf(
                "\n\nSQL Query\n"
            ));

            foreach ($warmQueries as $i => $query) {
                fwrite(STDERR, sprintf(
                    "------------------ \n%s;\n",
                    rtrim(preg_replace('/\s+/', ' ', $query['query'] ?? ''), ';')
                ));
            }

            fwrite(STDERR, "\n");
        }

        $this->assertSame($expected, $cold, 'cold cache result differs from native Eloquent');
        $this->assertSame($cold, $warm, 'warm cache result differs from cold');
    }

    protected function normalize(mixed $value): mixed
    {
        if ($value instanceof LengthAwarePaginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'total' => $value->total(),
                'current_page' => $value->currentPage(),
                'has_more' => $value->hasMorePages(),
            ];
        }

        if ($value instanceof Paginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'current_page' => $value->currentPage(),
                'has_more' => $value->hasMorePages(),
            ];
        }

        if ($value instanceof CursorPaginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'has_more' => $value->hasMorePages(),
                'cursor' => $value->cursor()?->toArray(),
            ];
        }

        if ($value instanceof EloquentCollection) {
            return $value->map->toArray()->values()->all();
        }

        if ($value instanceof Collection) {
            return $value->all(); // preserve keys (e.g. keyed pluck)
        }

        if ($value instanceof Model) {
            return $value->toArray();
        }

        return $value;
    }
}
