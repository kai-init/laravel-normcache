<?php

namespace NormCache\Tests\Integration\Infrastructure;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: Lua script edge cases — corrupt query entries are deleted and
 * re-queried, and cooldown-deferred version bumps fire on the next read.
 */
class LuaScriptBehaviorTest extends TestCase
{
    // Helpers

    private function redis()
    {
        return Redis::connection('normcache-test');
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
        $query = Author::query();

        return QueryHasher::forNormalizedQuery($query, $query->toBase());
    }

    public function test_corrupt_query_entry_is_deleted_and_treated_as_miss(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get();

        $version = NormCache::currentVersion(Author::class);
        $this->setKey("query:{{$ck}}:v{$version}:{$hash}", 'not-valid-json');

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(1, $results);
        $this->assertIsArray(json_decode($this->getKey("query:{{$ck}}:v{$version}:{$hash}"), true));
    }

    // luaFetchVersionedQuery — cooldown firing

    public function test_pending_cooldown_fires_version_bump_on_read(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);

        Author::get();
        $version = NormCache::currentVersion(Author::class);

        // Place a past-due scheduled invalidation directly in Redis
        $pastMs = (int) (microtime(true) * 1000) - 5000;
        $this->setKey("scheduled:{{$ck}}:", (string) $pastMs);

        Author::get(); // read triggers the Lua cooldown check — past-due scheduled key fires the bump

        $this->assertSame($version + 1, (int) $this->getKey("ver:{{$ck}}:"));
        $this->assertNull($this->getKey("scheduled:{{$ck}}:"));
    }

    public function test_non_numeric_scheduled_key_is_cleaned_up_without_version_bump(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);

        Author::get();
        $version = NormCache::currentVersion(Author::class);

        $this->setKey("scheduled:{{$ck}}:", 'garbage');

        Author::get();

        // Non-numeric value cannot be a valid timestamp — Lua cleans it without bumping the version.
        $this->assertSame($version, (int) $this->getKey("ver:{{$ck}}:"));
        $this->assertNull($this->getKey("scheduled:{{$ck}}:"));
    }
}
