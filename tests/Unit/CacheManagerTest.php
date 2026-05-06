<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\Tests\TestCase;

class CacheManagerTest extends TestCase
{
    private CacheManager $manager;

    protected function setUp(): void
    {
        parent::setUp();
        $this->manager = $this->cacheManager();
    }

    public function test_set_and_get_round_trip(): void
    {
        $this->manager->set('foo', 'bar');

        $this->assertSame('bar', $this->manager->get('foo'));
    }

    public function test_get_returns_null_for_missing_key(): void
    {
        $this->assertNull($this->manager->get('does-not-exist'));
    }

    public function test_delete_removes_value(): void
    {
        $this->manager->set('foo', 'bar');
        $this->manager->delete('foo');

        $this->assertNull($this->manager->get('foo'));
    }

    public function test_set_if_absent_succeeds_when_key_missing(): void
    {
        $result = $this->manager->setIfAbsent('lock', 1, 10);

        $this->assertTrue($result);
        $this->assertEquals(1, $this->manager->get('lock'));
    }

    public function test_set_if_absent_fails_when_key_present(): void
    {
        $this->manager->set('lock', 1);

        $result = $this->manager->setIfAbsent('lock', 2, 10);

        $this->assertFalse($result);
        $this->assertEquals(1, $this->manager->get('lock'));
    }

    public function test_get_many_returns_values_in_key_order(): void
    {
        $this->manager->set('a', 'alpha');
        $this->manager->set('c', 'gamma');

        $result = $this->manager->getMany(['a', 'b', 'c']);

        $this->assertSame(['alpha', null, 'gamma'], $result);
    }

    public function test_set_many_stores_multiple_values(): void
    {
        $this->manager->setMany(['x' => 10, 'y' => 20], 60);

        $this->assertEquals(10, $this->manager->get('x'));
        $this->assertEquals(20, $this->manager->get('y'));
    }

    public function test_current_version_returns_zero_before_any_invalidation(): void
    {
        $this->assertSame(0, $this->manager->currentVersion('SomeModel'));
    }

    public function test_invalidate_version_increments_version(): void
    {
        $this->manager->invalidateVersion('SomeModel');

        $this->assertSame(1, $this->manager->currentVersion('SomeModel'));
    }

    public function test_invalidate_version_called_twice_increments_twice(): void
    {
        $this->manager->invalidateVersion('SomeModel');
        $this->manager->invalidateVersion('SomeModel');

        $this->assertSame(2, $this->manager->currentVersion('SomeModel'));
    }

    public function test_invalidate_version_respects_cooldown(): void
    {
        $manager = new CacheManager(
            'model-cache-test',
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            config('normcache.key_prefix'),
            60,
        );

        $manager->invalidateVersion('SomeModel');
        $manager->invalidateVersion('SomeModel');

        $this->assertSame(1, $manager->currentVersion('SomeModel'));
    }

    public function test_current_version_is_served_from_local_after_first_read(): void
    {
        $this->manager->currentVersion('SomeModel'); // populates local cache

        // Modify Redis directly, bypassing the manager
        Redis::connection('model-cache-test')->set('test:ver:somemodel', 99);

        // Should still return locally cached value, not the new Redis value
        $this->assertSame(0, $this->manager->currentVersion('SomeModel'));
    }

    public function test_flush_version_local_forces_redis_read_on_next_call(): void
    {
        $this->manager->currentVersion('SomeModel'); // populates local cache

        Redis::connection('model-cache-test')->set('test:ver:somemodel', 99);

        $this->manager->flushVersionLocal();

        $this->assertSame(99, $this->manager->currentVersion('SomeModel'));
    }

    public function test_invalidate_version_updates_local_with_new_value(): void
    {
        $this->manager->invalidateVersion('SomeModel'); // version = 1, local cache updated

        // Modify Redis directly to simulate external change
        Redis::connection('model-cache-test')->set('test:ver:somemodel', 99);

        // local cache should have 1 (from the invalidation), not 99
        $this->assertSame(1, $this->manager->currentVersion('SomeModel'));
    }

    public function test_class_key_falls_back_to_class_basename(): void
    {
        $key = $this->manager->classKey('App\\Models\\Commission');

        $this->assertSame('commission', $key);
    }

    public function test_class_key_uses_morph_map_when_registered(): void
    {
        Relation::morphMap(['post' => 'App\\Models\\Post']);

        $key = $this->manager->classKey('App\\Models\\Post');

        $this->assertSame('post', $key);

        Relation::morphMap([], false);
        $this->resetClassKeyCache();
    }

    public function test_model_key_returns_expected_format(): void
    {
        $key = $this->manager->modelKey('App\\Models\\Commission', 42);

        $this->assertSame('model:commission:42', $key);
    }

    public function test_flush_model_bumps_version_and_clears_model_and_agg_keys(): void
    {
        $this->manager->set('model:post:1', ['id' => 1]);
        $this->manager->set('model:post:2', ['id' => 2]);
        $this->manager->set('agg:post:1:count:*:comments:nc:v1', ['v' => 3]);
        $this->manager->set('query:v1:abc', [1, 2]);
        $versionBefore = $this->manager->currentVersion('App\\Models\\Post');

        $this->manager->flushModel('App\\Models\\Post');

        $this->assertNull($this->manager->get('model:post:1'));
        $this->assertNull($this->manager->get('model:post:2'));
        $this->assertNull($this->manager->get('agg:post:1:count:*:comments:nc:v1'));
        $this->assertNotNull($this->manager->get('query:v1:abc'));
        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion('App\\Models\\Post'));
    }

    public function test_flush_all_removes_all_package_keys_and_returns_count(): void
    {
        $this->manager->set('query:v1:abc', [1, 2]);
        $this->manager->set('model:post:1', ['id' => 1]);
        $this->manager->set('ver:post', 3);
        $this->manager->set('agg:post:1:count:*:posts:nc:v1', ['v' => 5]);
        $this->manager->set('cooldown:post', 1);
        $this->manager->set('building:query:v1:abc', 1);

        $deleted = $this->manager->flushAll();

        $this->assertSame(6, $deleted);
        $this->assertEmpty($this->redisKeys('test:*'));
    }

    public function test_flush_all_returns_zero_when_cache_is_empty(): void
    {
        $this->assertSame(0, $this->manager->flushAll());
    }

    public function test_set_stores_scalar_without_php_serialization(): void
    {
        $this->manager->set('num', 99);

        $raw = Redis::connection('model-cache-test')->get('test:num');

        $this->assertStringNotContainsString('i:', $raw);
        $this->assertEquals(99, $this->manager->get('num'));
    }
}
