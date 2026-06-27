<?php

namespace NormCache\Tests\Integration\Infrastructure;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: concurrent misses queue behind a build lock and serve from cache
 * when the leader populates it; exhausted waiters and crashed lock holders fall through
 * to the database.
 */
class StampedeProtectionTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);
        $app['config']->set('normcache.stampede_wait_ms', 200);
    }

    private function redis()
    {
        return Redis::connection('normcache-test');
    }

    private function setKey(string $key, string $value, ?int $ttl = null): void
    {
        $prefixed = '{nc}:test:' . $key;
        $ttl !== null
            ? $this->redis()->setex($prefixed, $ttl, $value)
            : $this->redis()->set($prefixed, $value);
    }

    private function authorQueryHash(): string
    {
        $query = Author::query();

        return QueryHasher::forNormalizedQuery($query, $query->toBase());
    }

    public function test_waiter_serves_from_cache_after_build_completes(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get();

        $this->redis()->incr("{nc}:test:ver:{$ck}:");
        $newVersion = NormCache::currentVersion(Author::class);

        $this->redis()->set("{nc}:test:building:{$ck}:v{$newVersion}:{$hash}", '1');
        $this->redis()->lpush("{nc}:test:wake:{$ck}:{$hash}", '1');
        $this->setKey("query:{$ck}:v{$newVersion}:{$hash}", json_encode([(string) Author::first()->id], JSON_THROW_ON_ERROR), 60);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertSame(0, $queryCount);
        $this->assertCount(1, $results);
    }

    public function test_budget_exhausted_falls_through_to_db(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        $this->redis()->incr("{nc}:test:ver:{$ck}:");
        $newVersion = NormCache::currentVersion(Author::class);
        $this->redis()->set("{nc}:test:building:{$ck}:v{$newVersion}:{$hash}", '1');

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(1, $results);
    }

    public function test_lock_holder_crash_falls_through_after_budget(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        $this->redis()->incr("{nc}:test:ver:{$ck}:");
        $newVersion = NormCache::currentVersion(Author::class);
        $this->redis()->set("{nc}:test:building:{$ck}:v{$newVersion}:{$hash}", '1');

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(1, $results);
    }

    public function test_first_miss_claims_building_lock_and_populates_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::get();

        $this->assertGreaterThan(0, $queryCount);
        $this->assertCount(2, $results);
        $this->assertGreaterThan(0, $this->redis()->llen("{nc}:test:wake:{$ck}:{$hash}"));

        $queryCount = 0;
        Author::get();
        $this->assertSame(0, $queryCount);
    }
}
