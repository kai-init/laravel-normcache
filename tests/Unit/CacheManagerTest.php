<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\SpacedPost;
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
        $manager = $this->buildManager(cooldown: 60);
        $redis = Redis::connection('normcache-test');
        $classKey = $manager->classKey(Author::class);
        $scheduledKey = "{nc}:test:scheduled:{$classKey}:";

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
        $manager = $this->buildManager(cooldown: 60);

        $classKey = $manager->classKey(Author::class);
        Redis::connection('normcache-test')->set(
            "{nc}:test:scheduled:{$classKey}:",
            (string) ((int) floor(microtime(true) * 1000) - 1000)
        );

        $this->assertSame(1, $manager->currentVersion(Author::class));
    }

    public function test_current_version_always_reads_from_redis(): void
    {
        Redis::connection('normcache-test')->set('{nc}:test:ver:' . DB::getDefaultConnection() . ':posts:', 99);

        $this->assertSame(99, $this->manager->currentVersion(Post::class));
    }

    public function test_keys_are_prefixed_with_hash_tag_and_key_prefix(): void
    {
        $manager = $this->buildManager();

        $classKey = $manager->classKey(Author::class);
        $keys = $manager->keys();

        $fullVerKey = $keys->verKey($classKey);
        $this->assertSame("{nc}:test:ver:{$classKey}:", $fullVerKey);

        $manager->getStore()->set($fullVerKey, 7, 60);

        $this->assertSame('7', Redis::connection('normcache-test')->get("{nc}:test:ver:{$classKey}:"));
        $this->assertSame(7, $manager->currentVersion(Author::class));
    }

    public function test_with_space_reuses_matching_active_space(): void
    {
        $content = $this->manager->spaceFor(SpacedPost::class);

        $seen = $this->manager->keys()->withSpace(
            $content,
            fn() => $this->manager->withSpace($content, fn() => $this->manager->keys()->activeSpace()?->name),
        );

        $this->assertSame('content', $seen);
    }

    // -------------------------------------------------------------------------
    // Flush operations
    // -------------------------------------------------------------------------

    public function test_flush_model_bumps_version_and_clears_related_keys(): void
    {
        $store = $this->manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';
        $authorsKey = DB::getDefaultConnection() . ':authors';

        $store->set("model:{{$postsKey}}:v0:1", ['id' => 1], 3600);
        $store->set("model:{{$postsKey}}:v0:2", ['id' => 2], 3600);
        $store->set("query:{{$postsKey}}:v1:abc", [1, 2], 3600);
        $store->set("model:{{$authorsKey}}:v0:1", ['id' => 1], 3600);

        $versionBefore = $this->manager->currentVersion(Post::class);

        $this->manager->forceFlushModel(Post::class);

        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion(Post::class));
        $this->assertNull($this->modelCacheEntry(Post::class, 1));
        $this->assertNull($this->modelCacheEntry(Post::class, 2));
        $this->assertNotNull($store->get("query:{{$postsKey}}:v1:abc"));
        $this->assertNotNull($store->get("model:{{$authorsKey}}:v0:1"));
    }

    public function test_flush_all_removes_all_package_keys_and_returns_count(): void
    {
        $store = $this->manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';

        $store->set("{nc}:test:query:{{$postsKey}}:v1:abc", [1, 2], 3600);
        $store->set("{nc}:test:model:{{$postsKey}}:v0:1", ['id' => 1], 3600);
        $store->set("{nc}:test:ver:{{$postsKey}}:", 3, 3600);
        $store->set("{nc}:test:through:{{$postsKey}}:author:v1:v1:abc", [1], 3600);
        $store->set("{nc}:test:scheduled:{{$postsKey}}:", (string) ((int) floor(microtime(true) * 1000) + 1000), 3600);
        $store->set("{nc}:test:building:query:{{$postsKey}}:v1:abc", 1, 3600);

        $deleted = $this->manager->flushAll();

        $this->assertSame(6, $deleted);
        $this->assertEmpty($this->redisKeys('*'));
    }

    public function test_flush_all_returns_zero_when_cache_is_empty(): void
    {
        $this->assertSame(0, $this->manager->flushAll());
    }

    public function test_flush_all_uses_wildcard_hash_tag_patterns_on_standalone_after_fresh_registry_boot(): void
    {
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('Standalone wildcard hash-tag flush path is not used in Redis Cluster mode.');
        }

        $this->app->forgetInstance(CacheSpaceRegistry::class);

        $store = $this->manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';

        $store->set("{nc:content}:test:query:{{$postsKey}}:v1:abc", [1, 2], 3600);

        $deleted = $this->manager->flushAll();

        $this->assertSame(1, $deleted);
        $this->assertEmpty($store->scanPattern('{nc:content}:test:*'));
    }

    public function test_standalone_space_resolution_does_not_write_registry_metadata(): void
    {
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('Redis Cluster mode intentionally writes space registry metadata for flush discovery.');
        }

        $this->app->forgetInstance(CacheSpaceRegistry::class);

        $this->app->make(CacheSpaceRegistry::class)->spacesForModel(SpacedPost::class);

        $this->assertSame([], $this->manager->getStore()->scanPattern('{nc:meta}:test:spaces'));
    }

    public function test_flush_all_can_target_one_space(): void
    {
        $store = $this->manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';

        $store->set("{nc}:test:query:{{$postsKey}}:v1:abc", [1], 3600);
        $store->set("{nc:content}:test:query:{{$postsKey}}:v1:def", [2], 3600);

        $deleted = $this->manager->flushAll('content');

        $this->assertSame(1, $deleted);
        $this->assertNotEmpty($store->scanPattern('{nc}:test:*'));
        $this->assertEmpty($store->scanPattern('{nc:content}:test:*'));
    }

    public function test_flush_all_removes_keys_when_redis_connection_prefix_is_enabled(): void
    {
        config()->set('database.redis.options.prefix', 'laravel:');
        Redis::purge('normcache-test');

        $manager = $this->buildManager();

        $store = $manager->getStore();
        $postsKey = DB::getDefaultConnection() . ':posts';

        $store->set("{nc}:test:query:{{$postsKey}}:v1:abc", [1, 2], 3600);
        $store->set("{nc}:test:model:{{$postsKey}}:1", ['id' => 1], 3600);
        $store->set("{nc}:test:ver:{{$postsKey}}:", 3, 3600);

        $deleted = $manager->flushAll();

        $this->assertSame(3, $deleted);
        $this->assertSame([], $store->scanPattern('{nc}:test:*'));

        Redis::purge('normcache-test');
        config()->set('database.redis.options.prefix', '');
    }

    public function test_targeted_update_bumps_version_and_makes_model_cache_unreachable(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = $this->manager->currentVersion(Author::class);

        Author::whereKey($author->id)->update(['name' => 'Alicia']);

        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion(Author::class));
        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
    }

    public function test_scheduled_invalidation_key_persists_until_processed(): void
    {
        $manager = $this->buildManager(cooldown: 1);

        $model = new Author;
        $classKey = $manager->classKey(Author::class);
        $scheduledKey = "{nc}:test:scheduled:{$classKey}:";
        $redis = Redis::connection('normcache-test');

        $manager->invalidateVersion($model);
        $firstDueAt = $redis->get($scheduledKey);
        $manager->invalidateVersion($model);

        $this->assertSame(1, $redis->exists($scheduledKey));
        $this->assertSame($firstDueAt, $redis->get($scheduledKey));
    }

    public function test_get_models_applies_due_cooldown_before_reading_model_cache(): void
    {
        $author = Author::create(['name' => 'Fresh']);
        $manager = $this->buildManager(cooldown: 60);
        $classKey = $manager->classKey(Author::class);
        $store = $manager->getStore();

        $store->setRaw($manager->keys()->verKey($classKey), '0', 3600);
        $store->set($manager->keys()->modelPrefix($classKey, 0) . $author->id, [
            'id' => $author->id,
            'name' => 'Stale',
        ], 3600);

        $store->setRaw(
            $manager->keys()->scheduledKey($classKey),
            (string) ((int) floor(microtime(true) * 1000) - 1000),
            3600,
        );

        $models = $manager->getModels([$author->id], Author::class, null, null, Author::query());

        $this->assertSame('Fresh', $models[0]->name);
        $this->assertSame(1, $manager->currentVersion(Author::class));
        $this->assertNull($store->getRaw($manager->keys()->scheduledKey($classKey)));
    }

    public function test_store_through_ids_returns_false_on_version_mismatch(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $throughKey = "through:{{$classKey}}:v5:through-hash";
        $buildingKey = "building:{{$classKey}}:v5:through-hash";
        $wakeKey = "wake:{{$classKey}}:through-hash";

        $store->setRaw($versionKey, '5', 3600);
        $store->setRaw($buildingKey, '1', 3600);
        $store->increment($versionKey);

        $stored = $this->cacheManager()->storeThroughIds(
            $throughKey,
            [1],
            ['through-1'],
            3600,
            $buildingKey,
            [$versionKey],
            ['5'],
            null,
            $wakeKey,
        );

        $this->assertFalse($stored);
        $this->assertNull($store->getRaw($throughKey));
        $this->assertNull($store->getRaw($buildingKey));
    }

    public function test_store_model_attrs_for_versioned_result_uses_matching_version_key(): void
    {
        $manager = $this->cacheManager();
        $store = $manager->getStore();
        $authorKey = $manager->classKey(Author::class);
        $postKey = $manager->classKey(Post::class);

        $store->setRaw($manager->keys()->verKey($authorKey), '9', 3600);
        $store->setRaw($manager->keys()->verKey($postKey), '4', 3600);

        $manager->storeModelAttrsForVersionedResult(
            Author::class,
            [1 => ['id' => 1, 'name' => 'Fresh']],
            [$manager->keys()->verKey($postKey), $manager->keys()->verKey($authorKey)],
            ['4', '9'],
        );

        $this->assertSame(
            ['id' => 1, 'name' => 'Fresh'],
            $store->get($manager->keys()->modelPrefix($authorKey, 9) . '1'),
        );
    }

    public function test_store_model_attrs_for_version_skips_stale_version_write(): void
    {
        $manager = $this->cacheManager();
        $store = $manager->getStore();
        $classKey = $manager->classKey(Author::class);

        $store->setRaw($manager->keys()->verKey($classKey), '5', 3600);
        $store->increment($manager->keys()->verKey($classKey));

        $manager->storeModelAttrsForVersion(Author::class, [1 => ['id' => 1, 'name' => 'Stale']], 5);

        $this->assertNull($store->get($manager->keys()->modelPrefix($classKey, 5) . '1'));
        $this->assertNull($store->get($manager->keys()->modelPrefix($classKey, 6) . '1'));
    }

    public function test_store_model_attrs_for_version_writes_when_version_matches(): void
    {
        $manager = $this->cacheManager();
        $store = $manager->getStore();
        $classKey = $manager->classKey(Author::class);

        $store->setRaw($manager->keys()->verKey($classKey), '5', 3600);

        $manager->storeModelAttrsForVersion(Author::class, [1 => ['id' => 1, 'name' => 'Fresh']], 5);

        $this->assertSame(
            ['id' => 1, 'name' => 'Fresh'],
            $store->get($manager->keys()->modelPrefix($classKey, 5) . '1')
        );
    }

    public function test_store_query_ids_skips_write_on_version_mismatch(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $queryKey = "query:{{$classKey}}:v5:abc123";
        $buildingKey = "building:{{$classKey}}:abc123";
        $wakeKey = "wake:{{$classKey}}:abc123";

        $store->setRaw($versionKey, '5', 3600);
        $store->setRaw($buildingKey, '1', 3600);
        $store->increment($versionKey); // now 6

        $this->cacheManager()->storeQueryIds($queryKey, [1, 2, 3], 3600, $buildingKey, [$versionKey], ['5'], null, $wakeKey);

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
        $wakeKey = "wake:{{$classKey}}:def456";

        $store->setRaw($versionKey, '5', 3600);
        $store->setRaw($buildingKey, '1', 3600);

        $this->cacheManager()->storeQueryIds($queryKey, [4, 5, 6], 3600, $buildingKey, [$versionKey], ['5'], null, $wakeKey);

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
        $wakeKey = "wake:{{$classKey}}:token-mismatch";

        $store->setRaw($versionKey, '5', 3600);
        $store->setRaw($buildingKey, 'new-owner', 3600);

        $this->cacheManager()->storeQueryIds($queryKey, [7, 8, 9], 3600, $buildingKey, [$versionKey], ['5'], 'old-owner', $wakeKey);

        $this->assertNull($store->getRaw($queryKey), 'Outdated builder must not write when lock token changed');
        $this->assertSame('new-owner', $store->getRaw($buildingKey), 'Outdated builder must not release a newer lock');
    }

    // -------------------------------------------------------------------------
    // storeQueryIds — corrupt/default path
    // -------------------------------------------------------------------------

    public function test_store_query_ids_writes_normally_with_building_key_and_wake_key(): void
    {
        $store = $this->manager->getStore();
        $classKey = $this->manager->classKey(Author::class);
        $buildingKey = "building:{{$classKey}}:write_with_building_key";
        $wakeKey = "wake:{{$classKey}}:write_with_building_key";
        $key = "query:{{$classKey}}:write_with_building_key";

        $store->setRaw($buildingKey, 'token', 3600);

        $this->manager->storeQueryIds($key, ['1', '2'], 60, $buildingKey, [], [], 'token', $wakeKey);

        $this->assertNotNull($store->getRaw($key), 'Non-CAS write should proceed when buildingKey is set');
    }

    // -------------------------------------------------------------------------
    // invalidateMultipleVersions
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

        $this->assertGreaterThan($versionBefore, $this->manager->currentVersion(Author::class));
        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertNull($this->modelCacheEntry(Post::class, $post->id));
    }

    public function test_store_versioned_result_does_not_write_or_release_when_building_token_mismatches(): void
    {
        $store = $this->cacheManager()->getStore();
        $classKey = $this->cacheManager()->classKey(Author::class);

        $versionKey = "ver:{{$classKey}}:";
        $resultKey = "result:{{$classKey}}:v5:token-mismatch";
        $buildingKey = "building:{{$classKey}}:v5:token-mismatch";
        $wakeKey = "wake:{{$classKey}}:token-mismatch";

        $store->setRaw($versionKey, '5', 3600);
        $store->setRaw($buildingKey, 'new-owner', 3600);

        $written = $this->cacheManager()->storeVersionedResult(
            $resultKey,
            [['id' => 1, 'name' => 'Old']],
            3600,
            [$versionKey],
            ['5'],
            $buildingKey,
            $wakeKey,
            'old-owner'
        );

        $this->assertFalse($written);
        $this->assertNull($store->getRaw($resultKey), 'Outdated builder must not write when lock token changed');
        $this->assertSame('new-owner', $store->getRaw($buildingKey), 'Outdated builder must not release a newer lock');
        $this->assertNull($store->getRaw($wakeKey), 'Outdated builder must not wake waiters for a lock it no longer owns');
    }

    public function test_is_enabled_true_by_default(): void
    {
        $this->assertTrue($this->manager->isEnabled());
    }
}
