<?php

namespace NormCache\Tests;

use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\CacheServiceProvider;
use NormCache\Support\CacheKeyBuilder;
use Orchestra\Testbench\TestCase as OrchestraTestCase;
use ReflectionProperty;

abstract class TestCase extends OrchestraTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Redis::connection('model-cache-test')->flushdb();
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

        $app['config']->set('database.redis.client', env('REDIS_CLIENT', 'phpredis'));
        $app['config']->set('database.redis.options.prefix', '');

        $app['config']->set('database.redis.model-cache-test', [
            'host' => env('REDIS_HOST', '127.0.0.1'),
            'port' => env('REDIS_PORT', 6379),
            'database' => 15,
            'password' => env('REDIS_PASSWORD', null),
        ]);

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
        return Redis::connection('model-cache-test')->keys($pattern);
    }

    protected function cacheManager(): CacheManager
    {
        return $this->app->make('normcache');
    }
}
