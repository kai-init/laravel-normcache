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
use NormCache\CacheManager;
use NormCache\CacheServiceProvider;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use Orchestra\Testbench\TestCase as OrchestraTestCase;
use Predis\Client;

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
                    } catch (\Exception) {
                        // Replicas reject FLUSHDB.
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
    }

    protected function getPackageProviders($app): array
    {
        return [CacheServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $database = sys_get_temp_dir() . '/normcache-tests-' . getmypid() . '.sqlite';

        if (is_file($database)) {
            unlink($database);
        }

        touch($database);

        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => $database,
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
            $app['config']->set('database.redis.clusters.normcache-test', array_map(
                static function (string $node): array {
                    [$host, $port] = explode(':', $node);

                    return [
                        'host' => $host,
                        'port' => $port,
                        'database' => 0,
                        'password' => env('REDIS_PASSWORD', null),
                    ];
                },
                $nodes,
            ));
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
        $app['config']->set('normcache.ttl', 3600);
        $app['config']->set('normcache.query_ttl', 60);
    }

    protected function defineDatabaseMigrations(): void
    {
        $this->loadMigrationsFrom(__DIR__ . '/Fixtures/database');
    }

    protected function cacheManager(): CacheManager
    {
        return $this->app->make(CacheManager::class);
    }

    protected function cacheStore(): RedisStore
    {
        return $this->app->make(RedisStore::class);
    }

    protected function cacheKeys(): CacheKeyBuilder
    {
        return $this->app->make(CacheKeyBuilder::class);
    }

    protected function cacheKeysMatching(string $needle): array
    {
        $connection = Redis::connection('normcache-test');
        $client = $connection->client();
        $keys = [];

        if ($client instanceof \RedisCluster) {
            foreach ($client->_masters() as $master) {
                $keys = [...$keys, ...$client->keys($master, '*')];
            }
        } elseif (class_exists(Client::class) && $client instanceof Client && $this->isClusterRun()) {
            foreach ($client as $node) {
                $keys = [...$keys, ...$node->keys('*')];
            }
        } else {
            $keys = (array) $connection->keys('*');
        }

        return array_values(array_filter(
            array_unique(array_map(strval(...), $keys)),
            static fn(string $key): bool => str_contains($key, $needle),
        ));
    }

    private function isClusterRun(): bool
    {
        return env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true;
    }

    /**
     * Assert native == cold == warm and return warm SQL for strategy-specific contracts.
     *
     * @return list<array<string, mixed>>
     */
    protected function contract(callable $cached, callable $native, bool $expectNoStrayQueries = false): array
    {
        $expected = $this->normalize($native());
        $cold = $this->normalize($cached());

        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            $warm = $this->normalize($cached());
            $strayQueries = DB::getQueryLog();
        } finally {
            DB::disableQueryLog();
        }

        $this->assertSame($expected, $cold, 'cold cache result differs from native Eloquent');
        $this->assertSame($cold, $warm, 'warm cache result differs from cold');

        if ($expectNoStrayQueries) {
            $this->assertSame([], $strayQueries, 'expected no SQL queries on the warm cache path');
        }

        return $strayQueries;
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
