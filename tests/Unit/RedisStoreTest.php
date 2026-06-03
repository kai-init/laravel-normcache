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
        $this->store = new RedisStore('default', 'test:', false, '{nc}:');
    }

    public function test_it_prefixes_keys(): void
    {
        $this->assertSame('{nc}:test:foo', $this->store->prefix('foo'));
    }

    public function test_it_prefixes_keys_in_slotting_mode(): void
    {
        $store = new RedisStore('default', 'test:', true, '');
        $this->assertSame('test:foo', $store->prefix('foo'));
    }

    public function test_it_can_set_and_get_values(): void
    {
        $this->store->set('foo', 'bar', 60);
        $this->assertSame('bar', $this->store->get('foo'));
    }

    public function test_it_can_set_and_get_json_values(): void
    {
        $this->store->setJson('foo', ['bar' => 'baz'], 60);
        $this->assertSame(['bar' => 'baz'], json_decode($this->store->getRaw('foo'), true));
    }

    public function test_it_can_set_nx(): void
    {
        $this->store->delete('foo');
        $this->store->setNx('foo', 'bar');
        $this->assertSame('bar', $this->store->get('foo'));

        $this->store->setNx('foo', 'baz');
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
        $store = new RedisStore('default', 'test:', true, '');

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

        $result = $this->store->eval($script, ['foo']);

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

    public function test_it_uses_evalsha_with_fallback(): void
    {
        $script = 'return ARGV[1]';

        // First call should use EVAL and cache SHA
        $result = $this->store->eval($script, [], ['hello']);
        $this->assertSame('hello', $result);

        // Second call should use EVALSHA (mocking this is hard with real redis,
        // but we can verify it still works)
        $result = $this->store->eval($script, [], ['world']);
        $this->assertSame('world', $result);
    }

    public function test_eval_returns_correct_result_on_first_call(): void
    {
        (new ReflectionProperty(RedisStore::class, 'shas'))->setValue(null, []);

        $store = new RedisStore('model-cache-test', '', false);
        $script = RedisScripts::get('fetch_version_with_cooldown');

        Redis::connection('model-cache-test')->setex('ver:{authors}:', 60, '7');

        $result = $store->eval($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        $this->assertSame('7', $result);
    }

    public function test_eval_recovers_after_redis_script_flush(): void
    {
        $store = new RedisStore('model-cache-test', '', false);
        $redis = Redis::connection('model-cache-test');
        $script = RedisScripts::get('fetch_version_with_cooldown');

        $redis->setex('ver:{authors}:', 60, '5');
        $store->eval($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        if ($redis instanceof \Illuminate\Redis\Connections\PhpRedisConnection) {
            $redis->rawCommand('SCRIPT', 'FLUSH');
        } else {
            $redis->script('flush');
        }

        $result = $store->eval($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        $this->assertSame('5', $result);
    }

    public function test_php_sha_cache_is_populated_after_first_eval(): void
    {
        (new ReflectionProperty(RedisStore::class, 'shas'))->setValue(null, []);

        $store = new RedisStore('model-cache-test', '', false);
        $script = RedisScripts::get('fetch_version_with_cooldown');

        $this->assertArrayNotHasKey($script, (new ReflectionProperty(RedisStore::class, 'shas'))->getValue());

        Redis::connection('model-cache-test')->setex('ver:{authors}:', 60, '2');
        $store->eval($script, ['ver:{authors}:', 'scheduled:{authors}:'], [(string) (time() * 1000)]);

        $shas = (new ReflectionProperty(RedisStore::class, 'shas'))->getValue();
        $this->assertArrayHasKey($script, $shas);
        $this->assertSame(sha1($script), $shas[$script]);
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
