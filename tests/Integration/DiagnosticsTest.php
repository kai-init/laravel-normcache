<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\CacheInvalidated;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class DiagnosticsTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Events',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_cold_warm_bypass_and_invalidation_emit_v4_events(): void
    {
        Event::fake([
            QueryCacheHit::class,
            QueryCacheMiss::class,
            QueryBypassed::class,
            CacheInvalidated::class,
        ]);

        DB::table('posts')->where('id', $this->postId)->get();
        DB::table('posts')->where('id', $this->postId)->get();
        DB::table('posts')->where('id', $this->postId)->withoutCache()->get();
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Changed']);

        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'explicit_without_cache',
        );
        Event::assertDispatched(CacheInvalidated::class);
    }

    public function test_unmarked_queries_do_not_emit_bypass_events(): void
    {
        Event::fake([QueryBypassed::class]);

        DB::query()->from('posts')->where('id', $this->postId)->get();

        Event::assertNotDispatched(QueryBypassed::class);
    }

    public function test_cursor_and_explain_emit_stable_bypass_reasons(): void
    {
        Event::fake([QueryBypassed::class]);

        DB::table('posts')->cursor()->all();
        DB::table('posts')->explain();

        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'streaming_cursor',
        );
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'explain_query',
        );
    }

    public function test_corrupt_exact_payload_is_deleted_and_self_heals_as_a_miss(): void
    {
        $query = fn() => DB::table('posts')
            ->where('id', $this->postId)
            ->select('title')
            ->get();
        $query();
        $key = $this->cacheKeysMatching(':e:v')[0] ?? null;

        $this->assertIsString($key);
        $this->cacheStore()->setRaw($key, 'corrupt', 60);
        Event::fake([QueryCacheMiss::class]);

        $query();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $query();
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        Event::assertDispatched(
            QueryCacheMiss::class,
            fn(QueryCacheMiss $event): bool => $event->reason === 'corrupt_payload',
        );
    }

    public function test_absent_canonical_row_repairs_without_reporting_corruption(): void
    {
        DB::table('posts')->orderBy('id')->get();
        $rowKey = $this->cacheKeysMatching(':r:g')[0] ?? null;

        $this->assertIsString($rowKey);
        $this->cacheStore()->delete($rowKey);
        Event::fake([QueryCacheMiss::class]);

        DB::table('posts')->orderBy('id')->get();

        Event::assertNotDispatched(
            QueryCacheMiss::class,
            fn(QueryCacheMiss $event): bool => $event->reason === 'corrupt_payload',
        );
    }
}
