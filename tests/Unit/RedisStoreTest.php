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
        $this->store = new RedisStore('normcache-test');
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
        $this->store->set('{t}:build:foo', '1', 60);
        $this->store->releaseBuilding('{t}:build:foo', '{t}:wake:foo');

        $this->assertNull($this->store->getRaw('{t}:build:foo'));
        $this->assertTrue($this->store->brpop('{t}:wake:foo', 1));
    }

    public function test_release_building_pushes_configured_wake_tokens(): void
    {
        $store = new RedisStore('normcache-test', wakeTokenCount: 3);

        $store->set('{t}:build:tokens', '1', 60);
        $store->releaseBuilding('{t}:build:tokens', '{t}:wake:tokens');

        $this->assertSame(3, (int) $store->script("return redis.call('LLEN', KEYS[1])", ['{t}:wake:tokens']));
    }

    public function test_it_can_get_many_values(): void
    {
        $this->store->set('{nc}:foo', 'bar', 60);
        $this->store->set('{nc}:baz', 'qux', 60);

        $results = $this->store->getMany(['{nc}:foo', '{nc}:baz', '{nc}:missing']);

        $this->assertSame(['bar', 'qux', null], $results);
    }

    public function test_predis_cluster_get_many_uses_one_same_slot_mget_command(): void
    {
        $connection = new class extends PredisClusterConnection
        {
            public array $commands = [];

            public array $values = [];

            public function __construct() {}

            public function command($method, array $parameters = [])
            {
                $this->commands[] = [$method, $parameters];

                return array_map(fn($key) => $this->values[$key] ?? null, $parameters);
            }
        };

        $store = new RedisStore('normcache-test');
        $connection->values = [
            '{nc}:foo' => $store->serialize('bar'),
            '{nc}:baz' => $store->serialize('qux'),
        ];
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $keys = ['{nc}:foo', '{nc}:baz', '{nc}:missing'];

        $this->assertSame(['bar', 'qux', null], $store->getMany($keys));
        $this->assertSame([['mget', $keys]], $connection->commands);
    }

    public function test_it_can_run_lua_scripts(): void
    {
        $script = "return redis.call('GET', KEYS[1])";
        $this->store->set('foo', 'bar', 60);

        $result = $this->store->script($script, ['foo']);

        $this->assertSame('bar', $this->store->unserialize($result));
    }

    public function test_it_can_set_many_if_version(): void
    {
        $this->store->delete(['{t}:ver:1', '{t}:key:1', '{t}:key:2']);
        $this->store->setRaw('{t}:ver:1', '1', 60);

        $attrs = [
            '{t}:key:1' => ['id' => 1, 'name' => 'Alice'],
            '{t}:key:2' => ['id' => 2, 'name' => 'Bob'],
        ];

        $this->store->setManyIfVersion($attrs, 60, '{t}:ver:1', 1);

        $this->assertSame(['id' => 1, 'name' => 'Alice'], $this->store->get('{t}:key:1'));
        $this->assertSame(['id' => 2, 'name' => 'Bob'], $this->store->get('{t}:key:2'));

        // Should NOT update if version mismatch
        $attrs2 = ['{t}:key:1' => ['id' => 1, 'name' => 'Charlie']];
        $this->store->setManyIfVersion($attrs2, 60, '{t}:ver:1', 2);

        $this->assertSame(['id' => 1, 'name' => 'Alice'], $this->store->get('{t}:key:1'));
    }

    public function test_it_can_set_many_if_version_with_lock_release(): void
    {
        $this->store->delete(['{t}:ver:2', '{t}:key:3', '{t}:key:4', '{t}:lock:2', '{t}:wake:2']);
        $this->store->setRaw('{t}:ver:2', '1', 60);
        $this->store->setNxEx('{t}:lock:2', 'tok', 60);

        $attrs = [
            '{t}:key:3' => ['id' => 3, 'name' => 'Dee'],
            '{t}:key:4' => ['id' => 4, 'name' => 'Eve'],
        ];

        $this->store->setManyIfVersion($attrs, 60, '{t}:ver:2', 1, '{t}:lock:2', '{t}:wake:2', 'tok');

        $this->assertSame(['id' => 3, 'name' => 'Dee'], $this->store->get('{t}:key:3'));
        $this->assertSame(['id' => 4, 'name' => 'Eve'], $this->store->get('{t}:key:4'));
        $this->assertNull($this->store->getRaw('{t}:lock:2'), 'build lock should be released after the write');
    }

    public function test_set_many_if_version_handles_large_script_batches(): void
    {
        $this->store->delete(['{t}:ver:large']);
        $this->store->setRaw('{t}:ver:large', '1', 60);

        $attrs = [];
        for ($i = 0; $i < 10000; $i++) {
            $attrs["{t}:key:large:{$i}"] = ['id' => $i, 'name' => "Name {$i}"];
        }

        $this->store->setManyIfVersion($attrs, 60, '{t}:ver:large', 1);

        $this->assertSame(['id' => 9999, 'name' => 'Name 9999'], $this->store->get('{t}:key:large:9999'));
    }

    public function test_set_many_if_version_script_chunks_internally(): void
    {
        $this->store->setRaw('{t}:ver:script-large', '1', 60);

        $count = 8200;
        $keys = [];
        $values = [];
        for ($i = 0; $i < $count; $i++) {
            $keys[] = "{t}:key:script-large:{$i}";
            $values[] = $this->store->serialize(['id' => $i]);
        }

        $result = $this->store->script(
            RedisScripts::get('store_model_attrs'),
            array_merge(['{t}:ver:script-large'], $keys),
            array_merge(['1', '60', (string) $count, ''], $values)
        );

        $this->assertSame($count, (int) $result);
    }

    public function test_it_skips_write_but_still_releases_on_version_mismatch(): void
    {
        $this->store->delete(['{t}:ver:3', '{t}:key:5', '{t}:lock:3', '{t}:wake:3']);
        $this->store->setRaw('{t}:ver:3', '2', 60);
        $this->store->setNxEx('{t}:lock:3', 'tok', 60);

        $this->store->setManyIfVersion(
            ['{t}:key:5' => ['id' => 5]], 60, '{t}:ver:3', 1, '{t}:lock:3', '{t}:wake:3', 'tok'
        );

        $this->assertNull($this->store->get('{t}:key:5'));
        $this->assertNull($this->store->getRaw('{t}:lock:3'), 'build lock should still be released even when the write is skipped');
    }

    public function test_it_releases_lock_unconditionally_when_there_is_nothing_to_write(): void
    {
        $this->store->delete(['{t}:lock:4', '{t}:wake:4']);
        $this->store->setNxEx('{t}:lock:4', 'tok', 60);

        $this->store->setManyIfVersion([], 60, '{t}:ver:4', 1, '{t}:lock:4', '{t}:wake:4', 'tok');

        $this->assertNull($this->store->getRaw('{t}:lock:4'));
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

    public function test_predis_cluster_delete_batches_keys_by_hash_tag(): void
    {
        $connection = new class extends PredisClusterConnection
        {
            public array $commands = [];

            public function __construct() {}

            public function command($method, array $parameters = [])
            {
                $this->commands[] = [$method, $parameters];

                return count($parameters);
            }
        };

        $store = new RedisStore('normcache-test');
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $store->delete([
            '{nc}:foo:1',
            '{nc:content}:foo:1',
            '{nc}:foo:2',
            'untagged:1',
            'untagged:2',
        ]);

        $this->assertSame([
            ['del', ['{nc}:foo:1', '{nc}:foo:2']],
            ['del', ['{nc:content}:foo:1']],
            ['del', ['untagged:1']],
            ['del', ['untagged:2']],
        ], $connection->commands);
    }

    public function test_scan_pattern_strips_connection_prefix_from_returned_keys(): void
    {
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('Connection prefix reconfiguration not supported in cluster mode.');
        }

        config()->set('database.redis.options.prefix', 'laravel:');
        Redis::purge('normcache-test');

        try {
            $store = new RedisStore('normcache-test');
            $store->set('test:query:abc', [1], 60);
            $store->set('test:query:def', [2], 60);
            $store->set('test:model:1', ['id' => 1], 60);

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
            $store = new RedisStore('normcache-test');
            $store->set('test:query:1', 'a', 60);
            $store->set('test:query:2', 'b', 60);
            $store->set('test:model:1', 'c', 60);

            $count = $store->flushByPatterns(['test:query:*']);

            $this->assertSame(2, $count);
            $this->assertNull($store->get('test:query:1'));
            $this->assertNull($store->get('test:query:2'));
            $this->assertSame('c', $store->get('test:model:1'));
        } finally {
            Redis::purge('normcache-test');
            config()->set('database.redis.options.prefix', '');
        }
    }

    public function test_flush_by_patterns_scans_each_phpredis_cluster_master_once_and_filters_keys(): void
    {
        $client = new class
        {
            public array $scans = [];

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
                $this->scans[] = [$node, $pattern];
                $cursor = 0;

                return match ($node) {
                    'node-a' => [
                        'laravel:{nc}:test:model:{testing:posts}:1',
                        'laravel:{nc}:test:other:keep',
                    ],
                    'node-b' => ['laravel:{nc}:test:query:{testing:posts}:v1:abc'],
                    default => [],
                };
            }
        };

        $connection = new class($client) extends PhpRedisClusterConnection
        {
            public array $unlinked = [];

            public function __construct(private object $redisClient) {}

            public function _prefix($key)
            {
                return 'laravel:' . $key;
            }

            public function client()
            {
                return $this->redisClient;
            }

            public function unlink($keys)
            {
                $this->unlinked[] = (array) $keys;

                return count((array) $keys);
            }
        };

        $store = new RedisStore('normcache-test');
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $deleted = $store->flushByPatterns(['{nc}:test:model:*', '{nc}:test:query:*']);

        $this->assertSame(2, $deleted);
        $this->assertSame([
            ['node-a', 'laravel:{nc}:test:*'],
            ['node-b', 'laravel:{nc}:test:*'],
        ], $client->scans);
        $this->assertSame([[
            '{nc}:test:model:{testing:posts}:1',
            '{nc}:test:query:{testing:posts}:v1:abc',
        ]], $connection->unlinked);
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
            $directClient->setex("model:{posts}:{$i}", 60, 'x');
        }

        $this->assertCount(2500, $directClient->keys('model:*'));

        // Use 'predis' cluster type so the client iterates over configured nodes
        // without requiring CLUSTER SLOTS (which standalone Redis doesn't support).
        $predisClient = new PredisClient([$standaloneConn], ['cluster' => 'predis']);
        $clusterConnection = new PredisClusterConnection($predisClient);

        $store = new RedisStore('normcache-test');
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $clusterConnection);

        $deleted = $store->flushByPatterns(['model:{posts}:*']);

        $this->assertSame(2500, $deleted);
        $this->assertEmpty($directClient->keys('model:*'));
    }

    public function test_predis_cluster_scan_targets_owner_for_concrete_hash_tag_pattern(): void
    {
        $owner = new class
        {
            public array $patterns = [];

            public function scan($cursor, array $options)
            {
                $this->patterns[] = $options['match'] ?? null;

                return ['0', ['{nc:content}:test:query:testing:posts:v1:abc']];
            }
        };

        $other = new class
        {
            public int $calls = 0;

            public function scan($cursor, array $options)
            {
                $this->calls++;

                return ['0', []];
            }
        };

        $connection = new class($owner, $other) extends PredisClusterConnection
        {
            public function __construct(private object $owner, private object $other) {}

            public function client()
            {
                return new class($this->owner, $this->other) implements \IteratorAggregate
                {
                    public function __construct(private object $owner, private object $other) {}

                    public function getOptions()
                    {
                        return new class
                        {
                            public $prefix = null;
                        };
                    }

                    public function getConnection()
                    {
                        return new class($this->owner)
                        {
                            public function __construct(private object $owner) {}

                            public function getConnectionBySlot($slot)
                            {
                                return $this->owner;
                            }
                        };
                    }

                    public function getIterator(): \Traversable
                    {
                        return new \ArrayIterator([$this->owner, $this->other]);
                    }
                };
            }
        };

        $store = new RedisStore('normcache-test');
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $this->assertSame(
            ['{nc:content}:test:query:testing:posts:v1:abc'],
            $store->scanPattern('{nc:content}:test:query:*')
        );
        $this->assertSame(['{nc:content}:test:query:*'], $owner->patterns);
        $this->assertSame(0, $other->calls);
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

        $store = new RedisStore('normcache-test');
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

        $store = new RedisStore('normcache-test');
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

        $store = new RedisStore('normcache-test');
        $script = RedisScripts::get('fetch_version_with_cooldown');

        Redis::connection('normcache-test')->setex('ver:{authors}:', 60, '7');

        $result = $store->script($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        $this->assertSame('7', $result);
    }

    public function test_php_sha_cache_is_populated_after_first_eval(): void
    {
        (new ReflectionProperty(RedisStore::class, 'shas'))->setValue(null, []);

        $store = new RedisStore('normcache-test');
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

        $store = new RedisStore('normcache-test');
        $serializer = (new ReflectionProperty($store, 'serializer'))->getValue($store);
        (new ReflectionProperty($serializer, 'igbinary'))->setValue($serializer, false);

        $this->assertNull($store->unserialize(igbinary_serialize(['id' => 42])));
    }
}
