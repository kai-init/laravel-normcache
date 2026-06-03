<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
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

    // -------------------------------------------------------------------------
    // RedisStore pass-through (basic I/O tests live in RedisStoreTest)
    // -------------------------------------------------------------------------

    public function test_store_get_many_returns_values_in_key_order(): void
    {
        $this->manager->getStore()->set('a', 'alpha', 60);
        $this->manager->getStore()->set('c', 'gamma', 60);

        $result = $this->manager->getStore()->getMany(['a', 'b', 'c']);

        $this->assertSame(['alpha', null, 'gamma'], $result);
    }

    // -------------------------------------------------------------------------
    // Version tracking
    // -------------------------------------------------------------------------

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

    public function test_invalidate_version_schedules_once_with_cooldown(): void
    {
        $manager = new CacheManager(
            'model-cache-test',
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            config('normcache.key_prefix'),
            60,
            slotting: true,
        );
        $redis = Redis::connection('model-cache-test');
        $classKey = $manager->classKey(Author::class);
        $scheduledKey = "test:scheduled:{{$classKey}}:";

        $manager->invalidateVersion(new Author);
        $firstDueAt = $redis->get($scheduledKey);

        $manager->invalidateVersion(new Author);
        $secondDueAt = $redis->get($scheduledKey);

        $this->assertNotFalse($firstDueAt);
        $this->assertSame($firstDueAt, $secondDueAt);
        $this->assertSame(0, $manager->currentVersion(Author::class));
    }

    public function test_current_version_applies_due_scheduled_invalidation(): void
    {
        $manager = new CacheManager(
            'model-cache-test',
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            config('normcache.key_prefix'),
            60,
            slotting: true,
        );

        $classKey = $manager->classKey(Author::class);
        Redis::connection('model-cache-test')->set(
            "test:scheduled:{{$classKey}}:",
            (string) ((int) floor(microtime(true) * 1000) - 1000)
        );

        $this->assertSame(1, $manager->currentVersion(Author::class));
    }

    public function test_current_version_always_reads_from_redis(): void
    {
        Redis::connection('model-cache-test')->set('test:ver:{' . DB::getDefaultConnection() . ':posts}:', 99);

        $this->assertSame(99, $this->manager->currentVersion(Post::class));
    }

    public function test_default_non_slotting_prefixes_all_keys_and_preserves_existing_prefix(): void
    {
        $manager = new CacheManager(
            'model-cache-test',
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            'test:',
            config('normcache.cooldown'),
            true,
            slotting: false,
        );

        $classKey = $manager->classKey(Author::class);
        $store = $manager->getStore();

        $store->set("ver:{{$classKey}}:", 7, 60);

        $this->assertSame("{nc}:test:ver:{{$classKey}}:", $store->prefix("ver:{{$classKey}}:"));
        $this->assertSame('7', Redis::connection('model-cache-test')->get("{nc}:test:ver:{{$classKey}}:"));
        $this->assertSame(7, $manager->currentVersion(Author::class));
    }

    // -------------------------------------------------------------------------
    // Flush operations
    // -------------------------------------------------------------------------

    public function test_flush_model_bumps_version_and_clears_related_keys(): void
    {
        $redis = Redis::connection('model-cache-test');
        $store = $this->manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';
        $authorsKey = DB::getDefaultConnection() . ':authors';

        $redis->sadd("test:members:model:{{$postsKey}}", "test:model:{{$postsKey}}:1", "test:model:{{$postsKey}}:2");
        $store->set("model:{{$postsKey}}:1", ['id' => 1], 3600);
        $store->set("model:{{$postsKey}}:2", ['id' => 2], 3600);
        $store->set("query:{{$postsKey}}:v1:abc", [1, 2], 3600);
        $store->set("model:{{$authorsKey}}:1", ['id' => 1], 3600);

        $versionBefore = $this->manager->currentVersion(Post::class);

        $this->manager->forceFlushModel(Post::class);

        $this->assertNull($store->get("model:{{$postsKey}}:1"));
        $this->assertNull($store->get("model:{{$postsKey}}:2"));
        $this->assertSame(0, $redis->exists("test:members:model:{{$postsKey}}"));
        $this->assertNotNull($store->get("query:{{$postsKey}}:v1:abc"));
        $this->assertNotNull($store->get("model:{{$authorsKey}}:1"));
        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion(Post::class));
    }

    public function test_flush_all_removes_all_package_keys_and_returns_count(): void
    {
        $store = $this->manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';

        $store->set("query:{{$postsKey}}:v1:abc", [1, 2], 3600);
        $store->set("model:{{$postsKey}}:1", ['id' => 1], 3600);
        $store->set("ver:{{$postsKey}}:", 3, 3600);
        $store->set("through:{{$postsKey}}:author:v1:v1:abc", [1], 3600);
        $store->set("scheduled:{{$postsKey}}:", (string) ((int) floor(microtime(true) * 1000) + 1000), 3600);
        $store->set("building:query:{{$postsKey}}:v1:abc", 1, 3600);
        Redis::connection('model-cache-test')->sadd("test:members:model:{{$postsKey}}", "test:model:{{$postsKey}}:1");

        $deleted = $this->manager->flushAll();

        $this->assertSame(7, $deleted);
        $this->assertEmpty($this->redisKeys('test:*'));
    }

    public function test_flush_all_returns_zero_when_cache_is_empty(): void
    {
        $this->assertSame(0, $this->manager->flushAll());
    }

    public function test_flush_all_removes_keys_when_redis_connection_prefix_is_enabled(): void
    {
        config()->set('database.redis.options.prefix', 'laravel:');
        Redis::purge('model-cache-test');

        $manager = new CacheManager(
            'model-cache-test',
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            config('normcache.key_prefix'),
            config('normcache.cooldown'),
            slotting: true,
        );

        $store = $manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';

        $store->set("query:{{$postsKey}}:v1:abc", [1, 2], 3600);
        $store->set("model:{{$postsKey}}:1", ['id' => 1], 3600);
        $store->set("ver:{{$postsKey}}:", 3, 3600);

        $deleted = $manager->flushAll();

        $this->assertSame(3, $deleted);
        $this->assertSame([], Redis::connection('model-cache-test')->keys('*'));

        Redis::purge('model-cache-test');
        config()->set('database.redis.options.prefix', '');
    }

    public function test_targeted_delete_outside_transaction_removes_membership_reference(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $classKey = $this->manager->classKey(Author::class);
        $memberKey = "test:members:model:{{$classKey}}";
        $modelKey = "test:model:{{$classKey}}:{$author->id}";
        $redis = Redis::connection('model-cache-test');

        $this->assertTrue((bool) $redis->sismember($memberKey, $modelKey));

        Author::whereKey($author->id)->update(['name' => 'Alicia']);

        $this->assertFalse((bool) $redis->sismember($memberKey, $modelKey));
    }

    public function test_scheduled_invalidation_key_persists_until_processed(): void
    {
        $manager = new CacheManager(
            'model-cache-test',
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            config('normcache.key_prefix'),
            1,
            slotting: true,
        );

        $model = new Author;
        $classKey = $manager->classKey(Author::class);
        $scheduledKey = "test:scheduled:{{$classKey}}:";
        $redis = Redis::connection('model-cache-test');

        $manager->invalidateVersion($model);
        $firstDueAt = $redis->get($scheduledKey);
        $manager->invalidateVersion($model);

        $this->assertSame(1, $redis->exists($scheduledKey));
        $this->assertSame($firstDueAt, $redis->get($scheduledKey));
    }

    public function test_store_query_ids_skips_write_on_version_mismatch(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $queryKey = "query:{{$classKey}}:v5:abc123";
        $buildingKey = "building:{{$classKey}}:abc123";

        $store->setNx($versionKey, '5');
        $store->setNx($buildingKey, '1');
        $store->increment($versionKey); // now 6

        $this->cacheManager()->storeQueryIds($queryKey, [1, 2, 3], 3600, $buildingKey, [$versionKey], ['5']);

        $this->assertNull($store->getRaw($queryKey), 'CAS skips write when version has been bumped');
        $this->assertNull($store->getRaw($buildingKey), 'Building lock released even when write is skipped');
    }

    public function test_store_query_ids_writes_when_version_matches(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $queryKey = "query:{{$classKey}}:v5:def456";
        $buildingKey = "building:{{$classKey}}:def456";

        $store->setNx($versionKey, '5');
        $store->setNx($buildingKey, '1');

        $this->cacheManager()->storeQueryIds($queryKey, [4, 5, 6], 3600, $buildingKey, [$versionKey], ['5']);

        $this->assertNotNull($store->getRaw($queryKey), 'CAS writes when version still matches');
        $this->assertNull($store->getRaw($buildingKey), 'Building lock released after successful write');
    }

    public function test_store_query_ids_does_not_write_or_release_when_building_token_mismatches(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $queryKey = "query:{{$classKey}}:v5:token-mismatch";
        $buildingKey = "building:{{$classKey}}:token-mismatch";

        $store->setNx($versionKey, '5');
        $store->setNx($buildingKey, 'new-owner');

        $this->cacheManager()->storeQueryIds($queryKey, [7, 8, 9], 3600, $buildingKey, [$versionKey], ['5'], 'old-owner');

        $this->assertNull($store->getRaw($queryKey), 'Stale builder must not write when lock token changed');
        $this->assertSame('new-owner', $store->getRaw($buildingKey), 'Stale builder must not release a newer lock');
    }

    // -------------------------------------------------------------------------
    // storeQueryIds — corrupt/default path (Issue 3)
    // -------------------------------------------------------------------------

    public function test_store_query_ids_skips_write_without_building_key_and_version_keys(): void
    {
        $store = $this->manager->getStore();
        $key = 'query:corrupt_default_path:abc';

        $this->manager->storeQueryIds($key, ['1', '2', '3'], 60, null, [], [], null);

        $this->assertNull($store->getRaw($key), 'Corrupt/default path must not write unprotected query IDs');
    }

    public function test_store_query_ids_writes_normally_with_building_key_only(): void
    {
        $store = $this->manager->getStore();
        $classKey = $this->manager->classKey(Author::class);
        $buildingKey = "building:{{$classKey}}:write_with_building_key";
        $key = "query:{{$classKey}}:write_with_building_key";

        $store->setNx($buildingKey, 'token');

        $this->manager->storeQueryIds($key, ['1', '2'], 60, $buildingKey, [], [], 'token');

        $this->assertNotNull($store->getRaw($key), 'Non-CAS write should proceed when buildingKey is set');
    }

    // -------------------------------------------------------------------------
    // invalidateMultipleVersions (Issue 5)
    // -------------------------------------------------------------------------

    public function test_invalidate_multiple_versions_bumps_version_for_each_class(): void
    {
        $this->manager->invalidateMultipleVersions([Author::class, Post::class]);

        $this->assertSame(1, $this->manager->currentVersion(Author::class));
        $this->assertSame(1, $this->manager->currentVersion(Post::class));
    }

    public function test_invalidate_multiple_versions_does_nothing_when_disabled(): void
    {
        $this->manager->disable();
        $this->manager->invalidateMultipleVersions([Author::class]);

        $this->assertSame(0, $this->manager->currentVersion(Author::class));
    }

    public function test_invalidate_multiple_versions_inside_transaction_queues_version_bumps(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Author::all();
        Post::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertNotNull($this->modelCacheEntry(Post::class, $post->id));

        $versionBefore = $this->manager->currentVersion(Author::class);

        DB::transaction(function () {
            $this->manager->invalidateMultipleVersions([Author::class, Post::class], 'testing');
        });

        // Version bumped after commit
        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion(Author::class));
        // Model payloads preserved (version bump only, not full flush)
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertNotNull($this->modelCacheEntry(Post::class, $post->id));
    }

    public function test_store_versioned_result_does_not_write_or_release_when_building_token_mismatches(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $resultKey = "result:{{$classKey}}:v5:token-mismatch";
        $buildingKey = "building:{{$classKey}}:v5:token-mismatch";
        $wakeKey = "wake:{{$classKey}}:token-mismatch";

        $store->setNx($versionKey, '5');
        $store->setNx($buildingKey, 'new-owner');

        $written = $this->cacheManager()->storeVersionedResult(
            $resultKey,
            [['id' => 1, 'name' => 'Stale']],
            3600,
            [$versionKey],
            ['5'],
            $buildingKey,
            $wakeKey,
            'old-owner'
        );

        $this->assertFalse($written);
        $this->assertNull($store->getRaw($resultKey), 'Stale builder must not write when lock token changed');
        $this->assertSame('new-owner', $store->getRaw($buildingKey), 'Stale builder must not release a newer lock');
        $this->assertNull($store->getRaw($wakeKey), 'Stale builder must not wake waiters for a lock it no longer owns');
    }
}
