<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class ContractHarnessTest extends TestCase
{
    public function test_without_cache_callback_neither_reads_nor_populates_eager_load_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post', 'author_id' => $author->id]);
        $native = fn() => Author::withoutCache()->with('posts')->get();
        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            $this->cacheManager()->withoutCache($native);
            $this->cacheManager()->withoutCache($native);
            $this->assertCount(4, DB::getQueryLog());
        } finally {
            DB::disableQueryLog();
        }

        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        Author::with('posts')->get();
        Event::assertDispatchedTimes(QueryCacheMiss::class, 2);
        Event::assertNotDispatched(QueryCacheHit::class);

        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            Author::with('posts')->get();
            $this->assertSame([], DB::getQueryLog());
        } finally {
            DB::disableQueryLog();
        }

        Event::assertDispatchedTimes(QueryCacheHit::class, 2);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_without_cache_callback_restores_cache_reads_after_an_exception(): void
    {
        Author::create(['name' => 'Alice']);

        try {
            $this->cacheManager()->withoutCache(
                static fn() => throw new \RuntimeException('Expected failure.'),
            );
        } catch (\RuntimeException $exception) {
            $this->assertSame('Expected failure.', $exception->getMessage());
        }

        $query = static fn() => Author::query()->get();
        $this->assertColdCacheMiss($query);
        $this->assertWarmCacheHit($query);
    }
}
