<?php

namespace NormCache\Tests\Review;

use Illuminate\Support\Facades\Redis;
use NormCache\Tests\TestCase;

abstract class ReviewBenchmarkCase extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('normcache.events', false);
    }

    protected function setUp(): void
    {
        parent::setUp();

        $connection = Redis::connection('normcache-test');

        foreach (glob(__DIR__ . '/../../src/Lua/*.lua') as $path) {
            $connection->command('script', ['load', file_get_contents($path)]);
        }
    }

    protected function time(int $iterations, callable $operation): float
    {
        $started = hrtime(true);

        for ($iteration = 0; $iteration < $iterations; $iteration++) {
            $operation();
        }

        return (hrtime(true) - $started) / 1000;
    }
}
