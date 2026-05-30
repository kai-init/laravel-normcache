<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use ReflectionProperty;

class LuaScriptConsistencyTest extends TestCase
{
    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
        return QueryHasher::fromQuery(Author::query()->toBase());
    }

    // -------------------------------------------------------------------------
    // luaFetchVersionWithCooldown — cooldown fires on version resolution
    //
    // This script is used by CacheManager::resolveCurrentVersion() when
    // cooldown > 0. It is distinct from the cooldown logic inside
    // luaFetchVersionedQuery (which runs on every read).
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // dependsOn blob — building key causes DB fallthrough (dependsOn path)
    //
    // In luaFetchVersionedQuery, a claimed building key triggers stale serving.
    // In luaFetchQueryWithDeps there is no stale serving, so a claimed building
    // key returns 'building' and the caller falls through to the DB directly.
    // -------------------------------------------------------------------------

    public function test_building_key_in_deps_query_causes_db_fallthrough(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();
        $authorVer = NormCache::currentVersion(Author::class);
        $postVer = NormCache::currentVersion(Post::class);

        // Simulate a concurrent request having claimed the building key
        $this->setKey("building:{{$ck}}:v{$authorVer}:v{$postVer}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::query()->dependsOn([Post::class])->get();

        $this->assertGreaterThan(0, $queryCount); // 'building' → no stale path → DB fallthrough
        $this->assertCount(1, $results);
    }

    // -------------------------------------------------------------------------
    // luaFetchVersionedQuery — stale serving depth boundary
    //
    // serve_stale() walks back at most 3 versions (v-1, v-2, v-3). A cache
    // entry exactly 3 versions old must be served; one 4 versions old must not.
    // -------------------------------------------------------------------------

    public function test_stale_serve_reaches_three_versions_back(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get(); // cache populated at current version v

        // Advance Redis version by 3 so the cached entry is exactly 3 behind
        $this->bumpVersionInRedis($ck, 3);

        // Claim building key so serve_stale is attempted instead of a fresh miss
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

        Author::get(); // cache populated at current version v

        // Advance by 4: the cached entry is now 4 versions behind, out of reach
        $this->bumpVersionInRedis($ck, 4);

        $this->setKey("building:{{$ck}}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount); // no stale found, falls through to DB
        $this->assertCount(1, $results);
    }

    // -------------------------------------------------------------------------
    // Cooldown invalidation across cache families
    // -------------------------------------------------------------------------

    public function test_cooldown_due_invalidation_applies_to_raw_depends_on_cache(): void
    {
        $this->setCooldown(1);

        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Visible', 'author_id' => $author->id, 'published' => true]);

        $query = fn() => Author::query()
            ->whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->get();

        $this->assertCount(1, $query());
        $this->assertNotEmpty($this->redisKeys('test:raw:*'));

        $post->update(['published' => false]);

        $postClassKey = NormCache::classKey(Post::class);
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$postClassKey}}:", (string) $pastMs);

        $this->assertSame('0', (string) ($this->getKey("ver:{{$postClassKey}}:") ?? '0'));

        $this->assertCount(0, $query());
        $this->assertSame('1', (string) $this->getKey("ver:{{$postClassKey}}:"));
        $this->assertNull($this->getKey("scheduled:{{$postClassKey}}:"));
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
        $this->assertNotEmpty($this->redisKeys('test:scalar:*'));

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

        $authorClassKey = NormCache::classKey(Author::class);
        $tagClassKey = NormCache::classKey(Tag::class);
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$authorClassKey}}:", (string) $pastMs);
        $this->setKey("scheduled:{{$tagClassKey}}:", (string) $pastMs);

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
        $this->assertNotEmpty($this->redisKeys('test:agg:*'));

        Post::create(['title' => 'Post 3', 'author_id' => $author->id]);

        $postClassKey = NormCache::classKey(Post::class);
        $pastMs = (int) floor(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$postClassKey}}:", (string) $pastMs);

        $this->assertSame(3, $query()->posts_count);
        $this->assertSame('1', (string) $this->getKey("ver:{{$postClassKey}}:"));
        $this->assertNull($this->getKey("scheduled:{{$postClassKey}}:"));
    }
}
