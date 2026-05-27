<?php

namespace NormCache\Tests\Unit;

use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Support\Facades\Redis;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use Predis\Client as PredisClient;
use ReflectionProperty;

class RedisStoreTest extends TestCase
{
    public function test_flush_by_patterns_scans_all_phpredis_cluster_masters(): void
    {
        $connection = new class extends PhpRedisClusterConnection
        {
            public array $unlinked = [];

            public function __construct() {}

            public function _prefix($key)
            {
                return 'laravel:' . $key;
            }

            public function client()
            {
                return new class
                {
                    public function _masters(): array
                    {
                        return ['node-a', 'node-b'];
                    }
                };
            }

            public function scan($cursor, $options = [])
            {
                return match ([$options['match'], $options['node']]) {
                    ['test:model:*', 'node-a'] => [0, ['laravel:test:model:{testing:posts}:1']],
                    ['test:model:*', 'node-b'] => false,
                    ['test:query:*', 'node-a'] => false,
                    ['test:query:*', 'node-b'] => [0, ['laravel:test:query:{testing:posts}:v1:abc']],
                    default => false,
                };
            }

            public function unlink(...$keys)
            {
                $this->unlinked[] = $keys;

                return count($keys);
            }
        };

        $store = new RedisStore('model-cache-test', 'test:', true);
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $deleted = $store->flushByPatterns(['model:*', 'query:*']);

        $this->assertSame(2, $deleted);
        $this->assertSame([
            ['test:model:{testing:posts}:1'],
            ['test:query:{testing:posts}:v1:abc'],
        ], $connection->unlinked);
    }

    public function test_flush_by_patterns_scans_all_nodes_on_predis_cluster(): void
    {
        $phpRedis = Redis::connection('model-cache-test');

        for ($i = 0; $i < 2500; $i++) {
            $phpRedis->setex("testscan:model:{posts}:{$i}", 60, 'x');
        }

        $this->assertCount(2500, $phpRedis->keys('testscan:*'));

        $predisClient = new PredisClient(
            [['scheme' => 'tcp', 'host' => '127.0.0.1', 'port' => 6379, 'database' => 15]],
            ['cluster' => 'predis']
        );
        $clusterConnection = new PredisClusterConnection($predisClient);

        $store = new RedisStore('model-cache-test', 'testscan:', false);
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $clusterConnection);

        $deleted = $store->flushByPatterns(['model:{posts}:*']);

        $this->assertSame(2500, $deleted);
        $this->assertEmpty($phpRedis->keys('testscan:*'));
    }

    public function test_unserialize_detects_format_by_magic_header(): void
    {
        if (!extension_loaded('igbinary')) {
            $this->markTestSkipped('igbinary extension not available in this environment');
        }

        $store = new RedisStore('model-cache-test', '', false);
        $data = ['x' => 1];

        $this->assertSame($data, $store->unserialize(igbinary_serialize($data)));
        $this->assertSame($data, $store->unserialize(serialize($data)));
    }

    public function test_set_many_tracked_if_version_writes_when_version_matches(): void
    {
        $store = new RedisStore('model-cache-test', 'cas:', false);
        $redis = Redis::connection('model-cache-test');

        $store->setNx('ver:{authors}:', '3');

        $store->setManyTrackedIfVersion(
            ['model:{authors}:1' => ['id' => 1, 'name' => 'Alice']],
            60,
            'members:model:{authors}',
            'ver:{authors}:',
            3
        );

        $this->assertSame(['id' => 1, 'name' => 'Alice'], $store->get('model:{authors}:1'));
        $this->assertTrue($redis->sismember('cas:members:model:{authors}', 'cas:model:{authors}:1'));
    }

    public function test_set_many_tracked_if_version_skips_when_version_mismatches(): void
    {
        $store = new RedisStore('model-cache-test', 'cas:', false);
        $redis = Redis::connection('model-cache-test');

        $store->setNx('ver:{authors}:', '4');

        $store->setManyTrackedIfVersion(
            ['model:{authors}:1' => ['id' => 1, 'name' => 'Stale']],
            60,
            'members:model:{authors}',
            'ver:{authors}:',
            3
        );

        $this->assertNull($store->get('model:{authors}:1'));
        $this->assertSame(0, $redis->exists('cas:members:model:{authors}'));
    }

    public function test_igbinary_blob_returns_null_when_extension_absent(): void
    {
        if (!extension_loaded('igbinary')) {
            $this->markTestSkipped('igbinary extension not available in this environment');
        }

        $store = new RedisStore('model-cache-test', '', false);
        (new ReflectionProperty($store, 'igbinary'))->setValue($store, false);

        $this->assertNull($store->unserialize(igbinary_serialize(['id' => 42])));
    }
}
