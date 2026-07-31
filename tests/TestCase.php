<?php

namespace NormCache\Tests;

use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\CacheServiceProvider;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\Concerns\CacheAssertions;
use Orchestra\Testbench\TestCase as OrchestraTestCase;
use Predis\Client;

abstract class TestCase extends OrchestraTestCase
{
    use CacheAssertions;

    protected function setUp(): void
    {
        parent::setUp();

        $redis = Redis::connection('normcache-test');
        $client = $redis->client();

        if ($this->isClusterRun()) {
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

        if ($this->isClusterRun()) {
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
        $app['config']->set('normcache.row_ttl', 3600);
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
}
