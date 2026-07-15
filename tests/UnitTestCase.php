<?php

namespace NormCache\Tests;

use NormCache\CacheServiceProvider;
use NormCache\Support\CacheKeyBuilder;
use Orchestra\Testbench\TestCase as OrchestraTestCase;

abstract class UnitTestCase extends OrchestraTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        CacheKeyBuilder::reset();
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

        $app['config']->set('database.redis.client', 'predis');
        $app['config']->set('database.redis.options.prefix', '');
        $app['config']->set('database.redis.normcache-test', [
            'host' => env('REDIS_HOST', '127.0.0.1'),
            'port' => env('REDIS_PORT', 6379),
            'database' => 15,
            'password' => env('REDIS_PASSWORD', null),
        ]);

        $app['config']->set('normcache.connection', 'normcache-test');
        $app['config']->set('normcache.enabled', true);
        $app['config']->set('normcache.events', true);
        $app['config']->set('normcache.key_prefix', 'test:');
        $app['config']->set('normcache.ttl', 3600);
        $app['config']->set('normcache.query_ttl', 60);
        $app['config']->set('normcache.cooldown', 0);
    }
}
