<?php

namespace NormCache\Tests;

use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Pagination\CursorPaginator;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\CacheServiceProvider;
use NormCache\Support\CacheKeyBuilder;
use Orchestra\Testbench\TestCase as OrchestraTestCase;
use Predis\Client;
use ReflectionProperty;

abstract class TestCase extends OrchestraTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $redis = Redis::connection('model-cache-test');
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
            $app['config']->set('database.redis.clusters.model-cache-test', array_map(function ($node) {
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
            $app['config']->set('database.redis.model-cache-test', [
                'host' => env('REDIS_HOST', '127.0.0.1'),
                'port' => env('REDIS_PORT', 6379),
                'database' => 15,
                'password' => env('REDIS_PASSWORD', null),
            ]);
        }

        $app['config']->set('normcache.connection', 'model-cache-test');
        $app['config']->set('normcache.enabled', true);
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

    /** Assert native == cold == warm for a given query. */
    protected function contract(callable $cached, callable $native): void
    {
        $expected = $this->normalize($native());
        $cold = $this->normalize($cached());
        $warm = $this->normalize($cached());

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
