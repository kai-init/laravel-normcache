<?php

namespace NormCache\Tests;

use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Facades\Schema;
use NormCache\Cache\CacheRuntime;
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

        // Migrations memoize cache state before Redis is flushed.
        $this->app->forgetScopedInstances();
    }

    protected function getPackageProviders($app): array
    {
        return [CacheServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $driver = (string) env('TEST_DB_DRIVER', 'sqlite');

        $app['config']->set('database.default', 'testing');
        $app['config']->set(
            'database.connections.testing',
            $driver === 'sqlite'
                ? $this->sqliteDatabaseConfig()
                : $this->serverDatabaseConfig($driver),
        );

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
        if (env('TEST_DB_DRIVER', 'sqlite') !== 'sqlite') {
            Schema::dropAllTables();
        }

        $this->loadMigrationsFrom(__DIR__ . '/Fixtures/database');
    }

    /** @return array<string, mixed> */
    private function sqliteDatabaseConfig(): array
    {
        $database = sys_get_temp_dir() . '/normcache-tests-' . getmypid() . '.sqlite';

        if (is_file($database)) {
            unlink($database);
        }

        touch($database);

        return [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
        ];
    }

    /** @return array<string, mixed> */
    private function serverDatabaseConfig(string $driver): array
    {
        $port = match ($driver) {
            'pgsql' => 5432,
            'sqlsrv' => 1433,
            default => 3306,
        };
        $username = match ($driver) {
            'pgsql' => 'postgres',
            'sqlsrv' => 'sa',
            default => 'root',
        };

        return [
            'driver' => $driver,
            'host' => env('TEST_DB_HOST', '127.0.0.1'),
            'port' => env('TEST_DB_PORT', $port),
            'database' => env('TEST_DB_DATABASE', 'normcache'),
            'username' => env('TEST_DB_USERNAME', $username),
            'password' => env('TEST_DB_PASSWORD', ''),
            'charset' => in_array($driver, ['mysql', 'mariadb'], true) ? 'utf8mb4' : 'utf8',
            'collation' => in_array($driver, ['mysql', 'mariadb'], true)
                ? 'utf8mb4_unicode_ci'
                : null,
            'prefix' => '',
            'prefix_indexes' => true,
            'search_path' => 'public',
            'sslmode' => 'prefer',
            'encrypt' => $driver === 'sqlsrv' ? 'no' : null,
            'trust_server_certificate' => $driver === 'sqlsrv',
        ];
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

    protected function expireEpochMemo(): void
    {
        $runtime = $this->app->make(CacheRuntime::class);
        $readAt = new \ReflectionProperty($runtime, 'epochReadAt');

        $this->assertNotNull(
            $readAt->getValue($runtime),
            'a read must record when it resolved the epoch, or nothing can expire it',
        );

        $readAt->setValue($runtime, microtime(true) - 3600);
    }
}
