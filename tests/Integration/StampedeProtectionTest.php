<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class StampedeProtectionTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);
        $app['config']->set('normcache.stampede_wait_ms', 200);
    }

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

    private function authorQueryHash(): string
    {
        return QueryHasher::fromQuery(Author::query()->toBase());
    }

    public function test_waiter_serves_from_cache_after_build_completes(): void
    {
        Author::create(['name' => 'Alice']);

        $ck = NormCache::classKey(Author::class);
        $hash = $this->authorQueryHash();

        Author::get();

        $this->redis()->incr("test:ver:{{$ck}}:");
        NormCache::flushVersionLocal();
        $newVersion = NormCache::currentVersion(Author::class);

        $this->redis()->set("test:building:{{$ck}}:{$hash}", '1');
        $this->redis()->lpush("test:wake:{{$ck}}:{$hash}", '1');
        $this->setKey("query:{{$ck}}:v{$newVersion}:{$hash}", json_encode([1]), 60);

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

        $this->redis()->incr("test:ver:{{$ck}}:");
        NormCache::flushVersionLocal();
        $this->redis()->set("test:building:{{$ck}}:{$hash}", '1');

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

        $this->redis()->incr("test:ver:{{$ck}}:");
        NormCache::flushVersionLocal();
        $this->redis()->set("test:building:{{$ck}}:{$hash}", '1');

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
        $this->assertGreaterThan(0, $this->redis()->llen("test:wake:{{$ck}}:{$hash}"));

        $queryCount = 0;
        NormCache::flushVersionLocal();
        Author::get();
        $this->assertSame(0, $queryCount);
    }
}
