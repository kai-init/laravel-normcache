<?php

namespace NormCache\Tests\Unit;

use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Support\Facades\Redis;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use Predis\Client as PredisClient;
use ReflectionProperty;

class RedisStoreTest extends TestCase
{
    private RedisStore $store;

    protected function setUp(): void
    {
        parent::setUp();
        $this->store = new RedisStore('normcache-test', 'test:', false, '{nc}:');
    }

    public function test_it_prefixes_keys(): void
    {
        $this->assertSame('{nc}:test:foo', $this->store->prefix('foo'));
    }

    public function test_it_prefixes_keys_in_slotting_mode(): void
    {
        $store = new RedisStore('normcache-test', 'test:', true, '');
        $this->assertSame('test:foo', $store->prefix('foo'));
    }

    public function test_it_can_set_and_get_values(): void
    {
        $this->store->set('foo', 'bar', 60);
        $this->assertSame('bar', $this->store->get('foo'));
    }

    public function test_it_can_set_nx_ex(): void
    {
        $this->store->delete('foo');
        $this->assertTrue($this->store->setNxEx('foo', 'bar', 60));
        $this->assertSame('bar', $this->store->get('foo'));

        $this->assertFalse($this->store->setNxEx('foo', 'baz', 60));
        $this->assertSame('bar', $this->store->get('foo'));
    }

    public function test_it_can_delete_keys(): void
    {
        $this->store->set('foo', 'bar', 60);
        $this->store->delete('foo');
        $this->assertNull($this->store->get('foo'));
    }

    public function test_it_can_increment_values(): void
    {
        $this->store->delete('foo');
        $this->assertSame(1, $this->store->increment('foo'));
        $this->assertSame(2, $this->store->increment('foo'));
    }

    public function test_it_can_release_building_locks(): void
    {
        $this->store->set('build:foo', '1', 60);
        $this->store->releaseBuilding('build:foo', 'wake:foo');

        $this->assertNull($this->store->getRaw('build:foo'));
        $this->assertTrue($this->store->brpop('wake:foo', 1));
    }

    public function test_it_can_get_many_values(): void
    {
        $this->store->set('foo', 'bar', 60);
        $this->store->set('baz', 'qux', 60);

        $results = $this->store->getMany(['foo', 'baz', 'missing']);

        $this->assertSame(['bar', 'qux', null], $results);
    }

    public function test_it_can_set_many_values(): void
    {
        $this->store->setMany(['foo' => 'bar', 'baz' => 'qux'], 60);

        $this->assertSame('bar', $this->store->get('foo'));
        $this->assertSame('qux', $this->store->get('baz'));
    }

    public function test_it_can_group_keys_by_tag_in_slotting_mode(): void
    {
        $store = new RedisStore('normcache-test', 'test:', true, '');

        $method = new \ReflectionMethod(RedisStore::class, 'groupByTag');
        $method->setAccessible(true);

        $keys = ['{user:1}:a', '{user:1}:b', '{user:2}:c', 'no-tag'];
        $groups = $method->invoke($store, $keys);

        $this->assertSame([
            'user:1' => ['{user:1}:a', '{user:1}:b'],
            'user:2' => ['{user:2}:c'],
            'no-tag' => ['no-tag'],
        ], $groups);
    }

    public function test_it_can_run_lua_scripts(): void
    {
        $script = "return redis.call('GET', KEYS[1])";
        $this->store->set('foo', 'bar', 60);

        $result = $this->store->script($script, ['foo']);

        $this->assertSame('bar', $this->store->unserialize($result));
    }

    public function test_it_can_set_many_tracked_if_version(): void
    {
        $this->store->delete(['member:1', 'ver:1', 'key:1', 'key:2']);
        $this->store->setRaw('ver:1', '1', 60);

        $attrs = [
            'key:1' => ['id' => 1, 'name' => 'Alice'],
            'key:2' => ['id' => 2, 'name' => 'Bob'],
        ];

        $this->store->setManyTrackedIfVersion($attrs, 60, 'member:1', 'ver:1', 1);

        $this->assertSame(['id' => 1, 'name' => 'Alice'], $this->store->get('key:1'));
        $this->assertSame(['id' => 2, 'name' => 'Bob'], $this->store->get('key:2'));

        // Should NOT update if version mismatch
        $attrs2 = ['key:1' => ['id' => 1, 'name' => 'Charlie']];
        $this->store->setManyTrackedIfVersion($attrs2, 60, 'member:1', 'ver:1', 2);

        $this->assertSame(['id' => 1, 'name' => 'Alice'], $this->store->get('key:1'));
    }

    public function test_it_can_flush_by_patterns(): void
    {
        $this->store->set('foo:1', 'a', 60);
        $this->store->set('foo:2', 'b', 60);
        $this->store->set('bar:1', 'c', 60);

        $count = $this->store->flushByPatterns(['foo:*']);

        $this->assertSame(2, $count);
        $this->assertNull($this->store->get('foo:1'));
        $this->assertNull($this->store->get('foo:2'));
        $this->assertSame('c', $this->store->get('bar:1'));
    }

    public function test_scan_pattern_strips_connection_prefix_from_returned_keys(): void
    {
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('Connection prefix reconfiguration not supported in cluster mode.');
        }

        config()->set('database.redis.options.prefix', 'laravel:');
        Redis::purge('normcache-test');

        try {
            $store = new RedisStore('normcache-test', 'test:', false);
            $store->set('query:abc', [1], 60);
            $store->set('query:def', [2], 60);
            $store->set('model:1', ['id' => 1], 60);

            $keys = $store->scanPattern('test:query:*');

            $this->assertNotEmpty($keys);
            foreach ($keys as $key) {
                $this->assertStringNotContainsString('laravel:', $key, 'scanPattern should strip connection prefix');
                $this->assertStringStartsWith('test:query:', $key);
            }
        } finally {
            Redis::purge('normcache-test');
            config()->set('database.redis.options.prefix', '');
        }
    }

    public function test_flush_by_patterns_works_with_connection_prefix(): void
    {
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('Connection prefix reconfiguration not supported in cluster mode.');
        }

        config()->set('database.redis.options.prefix', 'laravel:');
        Redis::purge('normcache-test');

        try {
            $store = new RedisStore('normcache-test', 'test:', false);
            $store->set('query:1', 'a', 60);
            $store->set('query:2', 'b', 60);
            $store->set('model:1', 'c', 60);

            $count = $store->flushByPatterns(['query:*']);

            $this->assertSame(2, $count);
            $this->assertNull($store->get('query:1'));
            $this->assertNull($store->get('query:2'));
            $this->assertSame('c', $store->get('model:1'));
        } finally {
            Redis::purge('normcache-test');
            config()->set('database.redis.options.prefix', '');
        }
    }

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
                    public function getOption($option)
                    {
                        return 'laravel:';
                    }

                    public function _masters(): array
                    {
                        return ['node-a', 'node-b'];
                    }

                    public function scan(&$cursor, $node, $pattern = null, $count = 0)
                    {
                        $prev = $cursor;
                        $cursor = 0;

                        return match ([$pattern, $node, $prev]) {
                            ['laravel:test:model:*', 'node-a', null] => ['laravel:test:model:{testing:posts}:1'],
                            ['laravel:test:query:*', 'node-b', null] => ['laravel:test:query:{testing:posts}:v1:abc'],
                            default => [],
                        };
                    }
                };
            }

            public function unlink($keys)
            {
                $this->unlinked[] = (array) $keys;

                return count((array) $keys);
            }
        };

        $store = new RedisStore('normcache-test', 'test:', true);
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
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('Client-side Predis sharding test requires a standalone node; cluster SCAN path is covered by ClusterModeTest flush tests.');
        }

        // Target a standalone node for both seeding and the fake cluster.
        // Derived from config so local dev environments with non-default ports are respected.
        $cfg = $this->app['config']['database.redis.normcache-test']
            ?? ['host' => '127.0.0.1', 'port' => 6379, 'database' => 15];
        $standaloneConn = [
            'scheme' => 'tcp',
            'host' => $cfg['host'] ?? '127.0.0.1',
            'port' => (int) ($cfg['port'] ?? 6379),
            'database' => (int) ($cfg['database'] ?? 15),
        ];

        $directClient = new PredisClient($standaloneConn);

        for ($i = 0; $i < 2500; $i++) {
            $directClient->setex("testscan:model:{posts}:{$i}", 60, 'x');
        }

        $this->assertCount(2500, $directClient->keys('testscan:*'));

        // Use 'predis' cluster type so the client iterates over configured nodes
        // without requiring CLUSTER SLOTS (which standalone Redis doesn't support).
        $predisClient = new PredisClient([$standaloneConn], ['cluster' => 'predis']);
        $clusterConnection = new PredisClusterConnection($predisClient);

        $store = new RedisStore('normcache-test', 'testscan:', false);
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $clusterConnection);

        $deleted = $store->flushByPatterns(['model:{posts}:*']);

        $this->assertSame(2500, $deleted);
        $this->assertEmpty($directClient->keys('testscan:*'));
    }

    public function test_predis_cluster_scan_deduplicates_keys_returned_by_multiple_nodes(): void
    {
        $connection = new class extends PredisClusterConnection
        {
            public function __construct() {}

            public function client()
            {
                return new class implements \IteratorAggregate
                {
                    public function getOptions()
                    {
                        return new class
                        {
                            public $prefix = null;
                        };
                    }

                    public function getIterator(): \Traversable
                    {
                        return new \ArrayIterator([
                            new class
                            {
                                public function scan($cursor, array $options)
                                {
                                    return ['0', ['test:model:{posts}:1', 'test:model:{posts}:2']];
                                }
                            },
                            new class
                            {
                                public function scan($cursor, array $options)
                                {
                                    return ['0', ['test:model:{posts}:1']];
                                }
                            },
                        ]);
                    }
                };
            }
        };

        $store = new RedisStore('normcache-test', '', true);
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $this->assertSame(
            ['test:model:{posts}:1', 'test:model:{posts}:2'],
            $store->scanPattern('test:model:*')
        );
    }

    public function test_unserialize_detects_format_by_magic_header(): void
    {
        if (!extension_loaded('igbinary')) {
            $this->markTestSkipped('igbinary extension not available in this environment');
        }

        $store = new RedisStore('normcache-test', '', false);
        $data = ['x' => 1];

        $this->assertSame($data, $store->unserialize(igbinary_serialize($data)));
        $this->assertSame($data, $store->unserialize(serialize($data)));
    }

    public function test_it_uses_evalsha_with_fallback(): void
    {
        $script = 'return ARGV[1]';

        // Pass a dummy key so Predis cluster can route the command to a slot.
        // The script ignores KEYS and only reads ARGV[1].
        $result = $this->store->script($script, ['foo'], ['hello']);
        $this->assertSame('hello', $result);

        $result = $this->store->script($script, ['foo'], ['world']);
        $this->assertSame('world', $result);
    }

    public function test_eval_returns_correct_result_on_first_call(): void
    {
        (new ReflectionProperty(RedisStore::class, 'shas'))->setValue(null, []);

        $store = new RedisStore('normcache-test', '', false);
        $script = RedisScripts::get('fetch_version_with_cooldown');

        Redis::connection('normcache-test')->setex('ver:{authors}:', 60, '7');

        $result = $store->script($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        $this->assertSame('7', $result);
    }

    public function test_php_sha_cache_is_populated_after_first_eval(): void
    {
        (new ReflectionProperty(RedisStore::class, 'shas'))->setValue(null, []);

        $store = new RedisStore('normcache-test', '', false);
        $script = RedisScripts::get('fetch_version_with_cooldown');

        $this->assertArrayNotHasKey($script, (new ReflectionProperty(RedisStore::class, 'shas'))->getValue());

        Redis::connection('normcache-test')->setex('ver:{authors}:', 60, '2');
        $store->script($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        $shas = (new ReflectionProperty(RedisStore::class, 'shas'))->getValue();
        $this->assertArrayHasKey($script, $shas);
        $this->assertSame(sha1($script), $shas[$script]);
    }

    public function test_igbinary_blob_returns_null_when_extension_absent(): void
    {
        if (!extension_loaded('igbinary')) {
            $this->markTestSkipped('igbinary extension not available in this environment');
        }

        $store = new RedisStore('normcache-test', '', false);
        $serializer = (new ReflectionProperty($store, 'serializer'))->getValue($store);
        (new ReflectionProperty($serializer, 'igbinary'))->setValue($serializer, false);

        $this->assertNull($store->unserialize(igbinary_serialize(['id' => 42])));
    }
}
