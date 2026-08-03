<?php

namespace NormCache\Tests\Unit;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\Redis;
use NormCache\Support\RedisStore;
use NormCache\Tests\UnitTestCase;
use Predis\Connection\ConnectionException;
use Predis\Connection\NodeConnectionInterface;

final class RedisStoreRecoveryTest extends UnitTestCase
{
    public function test_rebuilds_the_connection_after_the_server_went_away(): void
    {
        $manager = $this->swapRedisManager([
            new \RuntimeException('Redis server went away'),
            'cached-value',
        ]);

        $store = new RedisStore('normcache-test');

        $this->assertSame('cached-value', $store->getRaw('key'));
        $this->assertSame(2, $manager->built);
        $this->assertSame(['normcache-test'], $manager->purged);
    }

    public function test_rebuilds_the_connection_after_a_predis_connection_failure(): void
    {
        $manager = $this->swapRedisManager([
            new ConnectionException(
                $this->createStub(NodeConnectionInterface::class),
                'Error while reading line from the server.',
            ),
            'cached-value',
        ]);

        $store = new RedisStore('normcache-test');

        $this->assertSame('cached-value', $store->getRaw('key'));
        $this->assertSame(2, $manager->built);
    }

    public function test_rebuilds_the_connection_for_lua_scripts(): void
    {
        $manager = $this->swapRedisManager([
            new \RuntimeException('Connection lost'),
            ['payload'],
        ]);

        $store = new RedisStore('normcache-test');

        $this->assertSame(
            ['payload'],
            $store->fetchResult('version-key', 'prefix', 'namespace', 'query-hash'),
        );
        $this->assertSame(2, $manager->built);
    }

    public function test_rebuilds_the_connection_for_increments(): void
    {
        $manager = $this->swapRedisManager([
            new \RuntimeException('Connection lost'),
            7,
        ]);

        $store = new RedisStore('normcache-test');

        $this->assertSame(7, $store->increment('key'));
        $this->assertSame(2, $manager->built);
    }

    public function test_claim_retry_recognizes_a_token_applied_before_connection_loss(): void
    {
        $original = $this->app->make('redis');
        $manager = new class
        {
            public int $built = 0;

            public ?string $owner = null;

            /** @var list<string> */
            public array $purged = [];

            public function connection($name = null): Connection
            {
                $attempt = $this->built++;
                $manager = $this;
                $client = new class($manager, $attempt)
                {
                    public function __construct(
                        private object $manager,
                        private int $attempt,
                    ) {}

                    /** @param list<mixed> $arguments */
                    public function __call(string $method, array $arguments): mixed
                    {
                        if (strtolower($method) !== 'evalsha') {
                            return null;
                        }

                        $token = (string) ($arguments[3] ?? '');
                        $this->manager->owner ??= $token;

                        if ($this->attempt === 0) {
                            throw new \RuntimeException('Connection lost after the lease was claimed.');
                        }

                        return [1, $this->manager->owner];
                    }
                };

                return new class($client) extends Connection
                {
                    public function __construct(mixed $client)
                    {
                        $this->client = $client;
                    }

                    public function createSubscription($channels, \Closure $callback, $method = 'subscribe'): void {}
                };
            }

            public function purge(string $name): void
            {
                $this->purged[] = $name;
            }
        };

        try {
            $this->app->instance('redis', $manager);
            Redis::clearResolvedInstance('redis');
            $token = str_repeat('a', 32);

            $this->assertSame(
                [true, $token],
                (new RedisStore('normcache-test'))->claimBuild('build-key', $token, 30),
            );
            $this->assertSame($token, $manager->owner);
            $this->assertSame(2, $manager->built);
            $this->assertSame(['normcache-test'], $manager->purged);
        } finally {
            $this->app->instance('redis', $original);
            Redis::clearResolvedInstance('redis');
        }
    }

    public function test_monotonic_increment_retry_may_advance_more_than_once(): void
    {
        $original = $this->app->make('redis');
        $manager = new class
        {
            public int $built = 0;

            public int $value = 0;

            /** @var list<string> */
            public array $purged = [];

            public function connection($name = null): Connection
            {
                $attempt = $this->built++;
                $manager = $this;
                $client = new class($manager, $attempt)
                {
                    public function __construct(
                        private object $manager,
                        private int $attempt,
                    ) {}

                    /** @param list<mixed> $arguments */
                    public function __call(string $method, array $arguments): mixed
                    {
                        if (strtolower($method) !== 'incr') {
                            return null;
                        }

                        $this->manager->value++;

                        if ($this->attempt === 0) {
                            throw new \RuntimeException('Connection lost after Redis applied INCR.');
                        }

                        return $this->manager->value;
                    }
                };

                return new class($client) extends Connection
                {
                    public function __construct(mixed $client)
                    {
                        $this->client = $client;
                    }

                    public function createSubscription($channels, \Closure $callback, $method = 'subscribe'): void {}
                };
            }

            public function purge(string $name): void
            {
                $this->purged[] = $name;
            }
        };

        try {
            $this->app->instance('redis', $manager);
            Redis::clearResolvedInstance('redis');

            $this->assertSame(2, (new RedisStore('normcache-test'))->increment('counter'));
            $this->assertSame(2, $manager->value);
            $this->assertSame(2, $manager->built);
            $this->assertSame(['normcache-test'], $manager->purged);
        } finally {
            $this->app->instance('redis', $original);
            Redis::clearResolvedInstance('redis');
        }
    }

    public function test_rebuilds_the_connection_for_deletes(): void
    {
        $manager = $this->swapRedisManager([
            new \RuntimeException('Connection lost'),
            1,
        ]);

        $store = new RedisStore('normcache-test');
        $store->delete(['key']);

        $this->assertSame(2, $manager->built);
    }

    public function test_surfaces_server_errors_after_a_single_retry(): void
    {
        // Server errors are retried too rather than pattern-matched away: one
        // wasted round trip is cheaper than misreading a lost socket as one.
        $error = new \RuntimeException('WRONGTYPE Operation against a key holding the wrong kind of value');
        $manager = $this->swapRedisManager([$error, $error]);

        $store = new RedisStore('normcache-test');

        $this->expectExceptionMessage('WRONGTYPE');

        try {
            $store->getRaw('key');
        } finally {
            $this->assertSame(2, $manager->built);
        }
    }

    public function test_does_not_retry_or_purge_for_programming_errors(): void
    {
        $manager = $this->swapRedisManager([
            new \TypeError('Argument #1 ($key) must be of type string, array given'),
            'cached-value',
        ]);

        $store = new RedisStore('normcache-test');

        $this->expectException(\TypeError::class);

        try {
            $store->getRaw('key');
        } finally {
            $this->assertSame(1, $manager->built);
            $this->assertSame([], $manager->purged);
        }
    }

    public function test_gives_up_when_the_rebuilt_connection_also_fails(): void
    {
        $manager = $this->swapRedisManager([
            new \RuntimeException('Redis server went away'),
            new \RuntimeException('Connection refused'),
        ]);

        $store = new RedisStore('normcache-test');

        $this->expectExceptionMessage('Connection refused');

        try {
            $store->getRaw('key');
        } finally {
            $this->assertSame(2, $manager->built);
        }
    }

    /** @param list<mixed> $responses */
    private function swapRedisManager(array $responses): object
    {
        $manager = new class($responses)
        {
            public int $built = 0;

            /** @var list<string> */
            public array $purged = [];

            /** @param list<mixed> $responses */
            public function __construct(private array $responses) {}

            public function connection($name = null): Connection
            {
                $response = $this->responses[$this->built] ?? null;
                $this->built++;

                $client = new class($response)
                {
                    public function __construct(private mixed $response) {}

                    /** @param list<mixed> $arguments */
                    public function __call(string $method, array $arguments): mixed
                    {
                        if ($this->response instanceof \Throwable) {
                            throw $this->response;
                        }

                        return $this->response;
                    }
                };

                return new class($client) extends Connection
                {
                    public function __construct(mixed $client)
                    {
                        $this->client = $client;
                    }

                    public function createSubscription($channels, \Closure $callback, $method = 'subscribe'): void {}
                };
            }

            public function purge(string $name): void
            {
                $this->purged[] = $name;
            }
        };

        $this->app->instance('redis', $manager);

        return $manager;
    }
}
