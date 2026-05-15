<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Facades\DB;
use NormCache\CacheManager;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
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
        $this->assertSame(0, $this->manager->currentVersion(Post::class));
    }

    public function test_invalidate_version_increments_version(): void
    {
        $this->manager->invalidateVersion(new Author);

        $this->assertSame(1, $this->manager->currentVersion(Author::class));
    }

    public function test_invalidate_version_called_twice_increments_twice(): void
    {
        $this->manager->invalidateVersion(new Author);
        $this->manager->invalidateVersion(new Author);

        $this->assertSame(2, $this->manager->currentVersion(Author::class));
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

        $manager->invalidateVersion(new Author);
        $manager->invalidateVersion(new Author);

        $this->assertSame(1, $manager->currentVersion(Author::class));
    }

    public function test_current_version_observes_cross_process_writes(): void
    {
        $this->manager->currentVersion(Post::class);

        Redis::connection('model-cache-test')->set('test:ver:{' . DB::getDefaultConnection() . ':posts}:', 99);

        $this->assertSame(99, $this->manager->currentVersion(Post::class));
    }

    public function test_flush_version_local_does_not_block_redis_reads(): void
    {
        $this->manager->currentVersion(Post::class);

        Redis::connection('model-cache-test')->set('test:ver:{' . DB::getDefaultConnection() . ':posts}:', 99);

        $this->manager->flushVersionLocal();

        $this->assertSame(99, $this->manager->currentVersion(Post::class));
    }

    public function test_invalidate_version_then_external_write_is_observed(): void
    {
        $this->manager->invalidateVersion(new Author);

        Redis::connection('model-cache-test')->set('test:ver:{' . DB::getDefaultConnection() . ':authors}:', 99);

        $this->assertSame(99, $this->manager->currentVersion(Author::class));
    }

    public function test_class_key_uses_table_name(): void
    {
        $default = DB::getDefaultConnection();

        $this->assertSame("{$default}:posts", $this->manager->classKey(Post::class));
        $this->assertSame("{$default}:authors", $this->manager->classKey(Author::class));
    }

    public function test_model_key_returns_expected_format(): void
    {
        $key = $this->manager->modelKey(Post::class, 42);

        $this->assertSame('model:{' . DB::getDefaultConnection() . ':posts}:42', $key);
    }

    public function test_flush_model_bumps_version_and_clears_related_keys(): void
    {
        $redis = Redis::connection('model-cache-test');
        $postsKey = DB::getDefaultConnection() . ':posts';
        $authorsKey = DB::getDefaultConnection() . ':authors';

        $redis->sadd("test:members:model:{{$postsKey}}", "test:model:{{$postsKey}}:1", "test:model:{{$postsKey}}:2");
        $this->manager->set("model:{{$postsKey}}:1", ['id' => 1]);
        $this->manager->set("model:{{$postsKey}}:2", ['id' => 2]);

        $this->manager->set("query:{{$postsKey}}:v1:abc", [1, 2]);
        $this->manager->set("agg:{{$postsKey}}:1:count:*:comments:nc:v1", ['v' => 3]);

        $this->manager->set("model:{{$authorsKey}}:1", ['id' => 1]);

        $versionBefore = $this->manager->currentVersion(Post::class);

        $this->manager->flushModel(Post::class);

        $this->assertNull($this->manager->get("model:{{$postsKey}}:1"));
        $this->assertNull($this->manager->get("model:{{$postsKey}}:2"));

        $this->assertSame(0, $redis->exists("test:members:model:{{$postsKey}}"));

        $this->assertNotNull($this->manager->get("query:{{$postsKey}}:v1:abc"));
        $this->assertNotNull($this->manager->get("agg:{{$postsKey}}:1:count:*:comments:nc:v1"));

        $this->assertNotNull($this->manager->get("model:{{$authorsKey}}:1"));

        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion(Post::class));
    }

    public function test_flush_all_removes_all_package_keys_and_returns_count(): void
    {
        $postsKey = DB::getDefaultConnection() . ':posts';

        $this->manager->set("query:{{$postsKey}}:v1:abc", [1, 2]);
        $this->manager->set("model:{{$postsKey}}:1", ['id' => 1]);
        $this->manager->set("ver:{{$postsKey}}:", 3);
        $this->manager->set("agg:{{$postsKey}}:1:count:*:posts:nc:v1", ['v' => 5]);
        $this->manager->set("through:{{$postsKey}}:author:v1:v1:abc", [1]);
        $this->manager->set("cooldown:{{$postsKey}}:", 1);
        $this->manager->set("building:query:{{$postsKey}}:v1:abc", 1);
        Redis::connection('model-cache-test')->sadd("test:members:model:{{$postsKey}}", "test:model:{{$postsKey}}:1");

        $deleted = $this->manager->flushAll();

        $this->assertSame(8, $deleted);
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
