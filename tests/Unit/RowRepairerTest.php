<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\CacheStateResolver;
use NormCache\Cache\RowRepairer;
use NormCache\Database\QueryBuilder;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheSerializer;
use NormCache\Support\FailureReporter;
use NormCache\Support\RedisStore;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheState;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

final class RowRepairerTest extends UnitTestCase
{
    public function test_recheck_failure_releases_the_owned_lease_after_retrying_mget(): void
    {
        $original = $this->app->make('redis');
        $manager = new class
        {
            public int $built = 0;

            public int $purged = 0;

            public int $releases = 0;

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
                        if (strtolower($method) === 'mget') {
                            throw new \RuntimeException('The repair recheck failed.');
                        }

                        if (strtolower($method) === 'evalsha') {
                            $buildingKey = (string) ($arguments[2] ?? '');

                            if (str_contains($buildingKey, ':build:x:') && count($arguments) > 5) {
                                $this->manager->releases++;

                                return 1;
                            }

                            return [1, (string) ($arguments[3] ?? '')];
                        }

                        return null;
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
                $this->purged++;
            }
        };

        try {
            $this->app->instance('redis', $manager);
            Redis::clearResolvedInstance('redis');

            $config = CacheConfig::fromArray(config('normcache'));
            $keys = new CacheKeyBuilder($config->keyPrefix);
            $store = new RedisStore($config->connection, $config->stampedeWakeTokens);
            $failures = new FailureReporter($this->app->make(LoggerInterface::class));
            $runtime = new CacheRuntime($config, $store, $keys, $failures);
            $repairer = new RowRepairer(
                $config,
                $store,
                $keys,
                new RawResultCodec(new CacheSerializer($config->serializer)),
                new CacheStateResolver($runtime, $store, $keys),
                new BuildLeaseCoordinator($config, $runtime, $store, $keys),
                $failures,
            );

            $table = TableIdentity::fromParts('sqlite', 'testing', ':memory:', '', '', 'authors');
            $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
            $plan = QueryPlan::canonical($table, [$table], $primaryKey);
            $state = new CacheState('query-key', '0', '0', '0', [], null, null);
            $database = DB::connection();
            $database->flushQueryLog();
            $database->enableQueryLog();
            $query = new QueryBuilder($database, null, new Processor);

            try {
                $repairer->repair($query, $plan, $state, ['i:1']);
                $this->fail('The repair recheck should throw.');
            } catch (\RuntimeException $exception) {
                $this->assertSame('The repair recheck failed.', $exception->getMessage());
            } finally {
                $database->disableQueryLog();
            }

            $this->assertSame([], $database->getQueryLog());
            $this->assertSame(2, $manager->built);
            $this->assertSame(1, $manager->purged);
            $this->assertSame(1, $manager->releases);
        } finally {
            $this->app->instance('redis', $original);
            Redis::clearResolvedInstance('redis');
        }
    }
}
