<?php

namespace NormCache\Tests\Unit;

use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use ReflectionProperty;

class RedisStoreTest extends TestCase
{
    public function test_flush_by_patterns_scans_all_phpredis_cluster_masters(): void
    {
        $connection = new class extends PhpRedisClusterConnection
        {
            public array $unlinked = [];

            public function __construct()
            {
            }

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
}
