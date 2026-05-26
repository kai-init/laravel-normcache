<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
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
        $pastMs = (int)(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$ck}}:", (string) $pastMs);

        $this->setCooldown(1);
        NormCache::flushVersionLocal();

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
        NormCache::flushVersionLocal();

        $version = NormCache::currentVersion(Author::class);

        $this->assertSame(3, $version);
        $this->assertNull($this->getKey("scheduled:{{$ck}}:"));
    }

    // -------------------------------------------------------------------------
    // luaFetchQueryWithDeps — corrupt entry (dependsOn path)
    //
    // luaFetchVersionedQuery handles corrupt entries in the standard path.
    // luaFetchQueryWithDeps must do the same for dependsOn() queries.
    // -------------------------------------------------------------------------

    public function test_corrupt_deps_query_entry_is_deleted_and_treated_as_miss(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::query()->dependsOn([Post::class])->get(); // populate cache

        $authorVer = NormCache::currentVersion(Author::class);
        $postVer   = NormCache::currentVersion(Post::class);
        $queryKey  = "query:{{$ck}}:v{$authorVer}:v{$postVer}:{$hash}";

        $this->setKey($queryKey, 'not-valid-json');
        NormCache::flushVersionLocal();

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) { $queryCount++; });

        $results = Author::query()->dependsOn([Post::class])->get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(1, $results);
        $this->assertIsArray(json_decode($this->getKey($queryKey), true)); // replaced with valid JSON
    }

    // -------------------------------------------------------------------------
    // luaFetchQueryWithDeps — building key causes DB fallthrough (dependsOn path)
    //
    // In luaFetchVersionedQuery, a claimed building key triggers stale serving.
    // In luaFetchQueryWithDeps there is no stale serving, so a claimed building
    // key returns 'building' and the caller falls through to the DB directly.
    // -------------------------------------------------------------------------

    public function test_building_key_in_deps_query_causes_db_fallthrough(): void
    {
        Author::create(['name' => 'Alice']);

        $ck       = NormCache::classKey(Author::class);
        $hash     = $this->authorQueryHash();
        $authorVer = NormCache::currentVersion(Author::class);
        $postVer   = NormCache::currentVersion(Post::class);

        NormCache::flushVersionLocal();

        // Simulate a concurrent request having claimed the building key
        $this->setKey("building:{{$ck}}:v{$authorVer}:v{$postVer}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) { $queryCount++; });

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

        $ck   = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get(); // cache populated at current version v
        NormCache::flushVersionLocal();

        // Advance Redis version by 3 so the cached entry is exactly 3 behind
        $this->bumpVersionInRedis($ck, 3);

        // Claim building key so serve_stale is attempted instead of a fresh miss
        $this->setKey("building:{{$ck}}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) { $queryCount++; });

        $results = Author::get();

        $this->assertSame(0, $queryCount);
        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }

    public function test_stale_serve_does_not_reach_four_versions_back(): void
    {
        Author::create(['name' => 'Alice']);

        $ck   = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get(); // cache populated at current version v
        NormCache::flushVersionLocal();

        // Advance by 4: the cached entry is now 4 versions behind, out of reach
        $this->bumpVersionInRedis($ck, 4);

        $this->setKey("building:{{$ck}}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) { $queryCount++; });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount); // no stale found, falls through to DB
        $this->assertCount(1, $results);
    }
}
