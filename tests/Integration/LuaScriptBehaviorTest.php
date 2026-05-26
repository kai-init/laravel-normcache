<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class LuaScriptBehaviorTest extends TestCase
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

    private function authorQueryHash(): string
    {
        return QueryHasher::fromQuery(Author::query()->toBase());
    }

    // -------------------------------------------------------------------------
    // luaFetchVersionedQuery — corrupt entry
    // -------------------------------------------------------------------------

    public function test_corrupt_query_entry_is_deleted_and_treated_as_miss(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get(); // populate cache at current version

        $version = NormCache::currentVersion(Author::class);
        $this->setKey("query:{{$ck}}:v{$version}:{$hash}", 'not-valid-json');
        NormCache::flushVersionLocal();

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) { $queryCount++; });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount); // treated as miss, DB queried
        $this->assertCount(1, $results);
        $this->assertIsArray(json_decode($this->getKey("query:{{$ck}}:v{$version}:{$hash}"), true)); // corrupt entry replaced with valid JSON
    }

    // -------------------------------------------------------------------------
    // luaFetchVersionedQuery — stale serving
    // -------------------------------------------------------------------------

    public function test_stale_result_served_when_building_key_is_active(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get(); // populate v1 cache

        // Bump version directly in Redis — simulates a write from another process
        $this->redis()->incr("test:ver:{{$ck}}:");
        NormCache::flushVersionLocal();

        // Simulate a concurrent request having already claimed the building key
        $this->setKey("building:{{$ck}}:{$hash}", '1', 30);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) { $queryCount++; });

        $results = Author::get();

        $this->assertSame(0, $queryCount); // stale v1 served — no DB round trip
        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }

    // -------------------------------------------------------------------------
    // luaFetchVersionedQuery — cooldown firing
    // -------------------------------------------------------------------------

    public function test_pending_cooldown_fires_version_bump_on_read(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);

        Author::get();
        $version = NormCache::currentVersion(Author::class);

        // Place a past-due scheduled invalidation directly in Redis
        $pastMs = (int)(microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$ck}}:", (string) $pastMs);
        NormCache::flushVersionLocal();

        Author::get(); // read triggers the Lua cooldown check

        $this->assertSame($version + 1, (int) $this->getKey("ver:{{$ck}}:")); // INCR fired
        $this->assertNull($this->getKey("scheduled:{{$ck}}:")); // scheduled key cleaned up
    }

    // -------------------------------------------------------------------------
    // luaFetchVersionedQuery — cooldown guard (fix 2)
    // -------------------------------------------------------------------------

    public function test_non_numeric_scheduled_key_is_cleaned_up_without_version_bump(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);

        Author::get();
        $version = NormCache::currentVersion(Author::class);

        $this->setKey("scheduled:{{$ck}}:", 'garbage');
        NormCache::flushVersionLocal();

        Author::get();

        $this->assertSame($version, (int) $this->getKey("ver:{{$ck}}:")); // version unchanged
        $this->assertNull($this->getKey("scheduled:{{$ck}}:")); // garbage key removed
    }
}
