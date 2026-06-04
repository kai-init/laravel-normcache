<?php

namespace NormCache\Tests\Integration\Infrastructure;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use ReflectionProperty;

/**
 * Behavioral tests: Lua script consistency under concurrent writes — result and pivot
 * cache writes are skipped when a dependency version changes during the build window;
 * stale serving reaches the configured depth but no further.
 */
class LuaScriptConsistencyTest extends TestCase
{
    // Helpers

    private function redis()
    {
        return Redis::connection('model-cache-test');
    }

    private function setKey(string $key, string $value, ?int $ttl = null): void
    {
        $prefixed = 'test:' . $key;
        $ttl !== null
            ? $this->redis()->setex($prefixed, $ttl, $value)
            : $this->redis()->set($prefixed, $value);
    }

    private function getKey(string $key): mixed
    {
        return $this->redis()->get('test:' . $key);
    }

    private function bumpVersionInRedis(string $classKey, int $times = 1): void
    {
        for ($i = 0; $i < $times; $i++) {
            $this->redis()->incr("test:ver:{{$classKey}}:");
        }
    }

    private function setCooldown(int $seconds): void
    {
        (new ReflectionProperty(CacheManager::class, 'cooldown'))
            ->setValue($this->cacheManager(), $seconds);
    }

    private function authorQueryHash(): string
    {
        return QueryHasher::forNormalizedQuery(Author::query());
    }

    // luaFetchVersionWithCooldown — cooldown fires on version resolution (used by CacheManager::resolveCurrentVersion() when cooldown > 0)

    public function test_cooldown_fires_version_bump_on_standalone_version_resolution(): void
    {
        $ck = NormCache::classKey(Author::class);

        $this->setKey("ver:{{$ck}}:", '3');
        $pastMs = (int) (microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$ck}}:", (string) $pastMs);

        $this->setCooldown(1);

        $version = NormCache::currentVersion(Author::class);

        $this->assertSame(4, $version);
        $this->assertNull($this->getKey("scheduled:{{$ck}}:"));
    }

    public function test_non_numeric_scheduled_key_cleaned_on_standalone_version_resolution(): void
    {
        $ck = NormCache::classKey(Author::class);

        $this->setKey("ver:{{$ck}}:", '3');
        $this->setKey("scheduled:{{$ck}}:", 'garbage');

        $this->setCooldown(1);

        $version = NormCache::currentVersion(Author::class);

        $this->assertSame(3, $version);
        $this->assertNull($this->getKey("scheduled:{{$ck}}:"));
    }

    // dependsOn blob — building key causes DB fallthrough (luaFetchQueryWithDeps has no stale path)

    public function test_building_key_in_deps_query_causes_db_fallthrough(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();
        $authorVer = NormCache::currentVersion(Author::class);
        $postVer = NormCache::currentVersion(Post::class);

        $this->setKey("building:{{$ck}}:v{$authorVer}:v{$postVer}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::query()->dependsOn([Post::class])->get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(1, $results);
    }

    // luaFetchVersionedQuery — stale serving depth boundary (walks back at most 3 versions)

    public function test_stale_serve_reaches_three_versions_back(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get();

        // serve_stale() walks back at most 3 versions; 3 bumps puts the entry at exactly the boundary
        $this->bumpVersionInRedis($ck, 3);

        // Building lock must be held so the Lua script attempts stale serving rather than a fresh miss
        $this->setKey("building:{{$ck}}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertSame(0, $queryCount);
        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }

    public function test_stale_serve_does_not_reach_four_versions_back(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get();

        // 4 bumps puts the entry one past the stale-serve depth limit of 3
        $this->bumpVersionInRedis($ck, 4);

        $this->setKey("building:{{$ck}}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(1, $results);
    }

    // Cooldown invalidation across cache families

    public function test_cooldown_due_invalidation_applies_to_result_depends_on_cache(): void
    {
        $this->setCooldown(1);

        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Visible', 'author_id' => $author->id, 'published' => true]);

        $query = fn() => Author::query()
            ->whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->get();

        $this->assertCount(1, $query());
        $this->assertNotEmpty($this->redisKeys('test:result:*'));

        $post->update(['published' => false]);

        $postClassKey = NormCache::classKey(Post::class);
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$postClassKey}}:", (string) $pastMs);

        // Post version is still 0 (never bumped) — the scheduled key is what triggers the bump
        $this->assertSame('0', (string) ($this->getKey("ver:{{$postClassKey}}:") ?? '0'));

        $this->assertCount(0, $query());
        $this->assertSame('1', (string) $this->getKey("ver:{{$postClassKey}}:"));
        $this->assertNull($this->getKey("scheduled:{{$postClassKey}}:"));
    }

    public function test_result_cache_write_is_skipped_when_dependency_version_changes_during_build(): void
    {
        $manager = $this->cacheManager();
        $hash = 'manual-result-build';

        $miss = $manager->getResultCache(Author::class, [Post::class], $hash);

        $this->assertSame('miss', $miss['status']);

        $this->bumpVersionInRedis(NormCache::classKey(Post::class));

        $manager->storeResultCache(
            $miss['key'],
            [['id' => 1, 'name' => 'Stale']],
            $miss['buildingKey'],
            60,
            $miss['wakeKey'],
            $miss['versionKeys'],
            $miss['expectedVersions'],
        );

        $this->assertNull($manager->getStore()->get($miss['key']));
        $this->assertNull($manager->getStore()->getRaw($miss['buildingKey']));
    }

    public function test_namespaced_result_write_is_skipped_when_dependency_version_changes_during_build(): void
    {
        $manager = $this->cacheManager();
        $cache = $manager->getResultCache(Author::class, [Post::class], 'manual-count-build', null, [], 'count');

        $this->bumpVersionInRedis(NormCache::classKey(Post::class));

        $manager->storeVersionedResult(
            $cache['key'],
            [10],
            60,
            $cache['versionKeys'],
            $cache['expectedVersions'],
        );

        $this->assertNull($manager->getStore()->get($cache['key']));
    }

    public function test_through_result_write_is_skipped_when_dependency_version_changes_during_build(): void
    {
        $manager = $this->cacheManager();
        $cache = $manager->getResultCache(Post::class, [Country::class], 'manual-through-build', null, [], 'through');

        $this->bumpVersionInRedis(NormCache::classKey(Country::class));

        $manager->storeVersionedResult(
            $cache['key'],
            ['ids' => [1], 'throughKeys' => [1 => 1]],
            60,
            $cache['versionKeys'],
            $cache['expectedVersions'],
        );

        $this->assertNull($manager->getStore()->get($cache['key']));
    }

    public function test_related_model_payload_is_not_cached_when_through_result_write_is_skipped(): void
    {
        $manager = $this->cacheManager();
        $cache = $manager->getResultCache(Post::class, [Country::class], 'manual-through-build', null, [], 'through');

        $this->bumpVersionInRedis(NormCache::classKey(Country::class));

        if ($manager->storeVersionedResult(
            $cache['key'],
            ['ids' => [1], 'throughKeys' => [1 => 1]],
            60,
            $cache['versionKeys'],
            $cache['expectedVersions'],
        )) {
            $manager->cacheModelAttrs(Post::class, [1 => ['id' => 1, 'title' => 'Stale']]);
        }

        $this->assertNull($this->modelCacheEntry(Post::class, 1));
    }

    public function test_pivot_result_write_is_skipped_when_dependency_version_changes_during_build(): void
    {
        $manager = $this->cacheManager();
        $pivotTableKey = $manager->tableKey(DB::getDefaultConnection(), 'author_tag');
        $cache = $manager->getPivotCache(Author::class, Tag::class, 'tags', [1], 'manual-pivot-build', $pivotTableKey);
        $authorKey = NormCache::classKey(Author::class);
        $tagKey = NormCache::classKey(Tag::class);
        $pivotKey = "pivot:{{$authorKey}}:{$tagKey}:tags:manual-pivot-build:{$cache['seg']}:1";

        $this->bumpVersionInRedis($pivotTableKey);

        $manager->storeVersionedResult(
            $pivotKey,
            [['id' => 1, 'pivot' => []]],
            60,
            $cache['versionKeys'],
            $cache['expectedVersions'],
        );

        $this->assertNull($manager->getStore()->get($pivotKey));
    }

    public function test_related_model_payload_is_not_cached_when_pivot_result_write_is_skipped(): void
    {
        $manager = $this->cacheManager();
        $pivotTableKey = $manager->tableKey(DB::getDefaultConnection(), 'author_tag');
        $cache = $manager->getPivotCache(Author::class, Tag::class, 'tags', [1], 'manual-pivot-build', $pivotTableKey);
        $authorKey = NormCache::classKey(Author::class);
        $tagKey = NormCache::classKey(Tag::class);
        $pivotKey = "pivot:{{$authorKey}}:{$tagKey}:tags:manual-pivot-build:{$cache['seg']}:1";

        $this->bumpVersionInRedis($tagKey);

        if ($manager->storeVersionedResult(
            $pivotKey,
            [['id' => 1, 'pivot' => []]],
            60,
            $cache['versionKeys'],
            $cache['expectedVersions'],
        )) {
            $manager->cacheModelAttrs(Tag::class, [1 => ['id' => 1, 'name' => 'Stale']]);
        }

        $this->assertNull($this->modelCacheEntry(Tag::class, 1));
    }

    public function test_cooldown_due_invalidation_applies_to_scalar_cache(): void
    {
        $this->setCooldown(1);

        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Visible', 'author_id' => $author->id, 'published' => true]);

        $query = fn() => Author::query()
            ->whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->count();

        $this->assertSame(1, $query());
        $this->assertNotEmpty($this->redisKeys('test:count:*'));
        $this->assertEmpty($this->redisKeys('test:result:*'));

        $post->update(['published' => false]);

        $postClassKey = NormCache::classKey(Post::class);
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$postClassKey}}:", (string) $pastMs);

        $this->assertSame(0, $query());
        $this->assertSame('1', (string) $this->getKey("ver:{{$postClassKey}}:"));
        $this->assertNull($this->getKey("scheduled:{{$postClassKey}}:"));
    }

    public function test_cooldown_due_invalidation_applies_to_pivot_cache(): void
    {
        $this->setCooldown(1);

        $author = Author::create(['name' => 'Alice']);
        $old = Tag::create(['name' => 'old']);
        $new = Tag::create(['name' => 'new']);

        $author->tags()->attach($old->id);

        $this->assertSame(['old'], $author->tags()->get()->pluck('name')->all());
        $this->assertNotEmpty($this->redisKeys('test:pivot:*'));

        $author->tags()->detach($old->id);
        $author->tags()->attach($new->id);

        $pivotTableKey = NormCache::tableKey($author->getConnection()->getName(), 'author_tag');
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$pivotTableKey}}:", (string) $pastMs);

        $this->assertSame(['new'], $author->tags()->get()->pluck('name')->all());
    }

    public function test_cooldown_due_invalidation_applies_to_aggregate_cache(): void
    {
        $this->setCooldown(1);

        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);
        Post::create(['title' => 'Post 2', 'author_id' => $author->id]);

        $query = fn() => Author::withCount('posts')->find($author->id);

        $this->assertSame(2, $query()->posts_count);

        Post::create(['title' => 'Post 3', 'author_id' => $author->id]);

        $postClassKey = NormCache::classKey(Post::class);
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$postClassKey}}:", (string) $pastMs);

        $this->assertSame(3, $query()->posts_count);
        $this->assertSame('1', (string) $this->getKey("ver:{{$postClassKey}}:"));
        $this->assertNull($this->getKey("scheduled:{{$postClassKey}}:"));
    }
}
