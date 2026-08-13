<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\CacheInvalidated;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;

final class DiagnosticsTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) RawPost::query()->toBase()->insertGetId([
            'title' => 'Events',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_cold_warm_bypass_and_invalidation_emit_events(): void
    {
        Event::fake([
            QueryCacheHit::class,
            QueryCacheMiss::class,
            QueryBypassed::class,
            CacheInvalidated::class,
        ]);

        RawPost::query()->toBase()->where('id', $this->postId)->get();
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        RawPost::query()->toBase()->where('id', $this->postId)->withoutCache()->get();
        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Changed']);

        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'explicit_without_cache',
        );
        Event::assertDispatched(
            CacheInvalidated::class,
            fn(CacheInvalidated $event): bool => $event->mode === 'precise'
                && $event->primaryKeyTokens === ['i:' . $this->postId],
        );
    }

    public function test_unmarked_queries_do_not_emit_bypass_events(): void
    {
        Event::fake([QueryBypassed::class]);

        DB::query()->from('posts')->where('id', $this->postId)->get();

        Event::assertNotDispatched(QueryBypassed::class);
    }

    public function test_cursor_and_explain_are_not_overridden_and_report_nothing(): void
    {
        Event::fake([QueryBypassed::class]);

        RawPost::query()->toBase()->cursor()->all();
        RawPost::query()->toBase()->explain();

        Event::assertNotDispatched(QueryBypassed::class);
    }

    public function test_execution_safety_bypass_reasons_remain_stable(): void
    {
        Event::fake([QueryBypassed::class]);

        RawPost::query()->toBase()->where('id', $this->postId)->withoutCache()->get();
        RawPost::query()->toBase()->where('id', $this->postId)->useWritePdo()->get();
        DB::transaction(fn() => RawPost::query()->toBase()->where('id', $this->postId)->get());
        RawPost::query()->toBase()->where('id', $this->postId)->lockForUpdate()->get();

        Event::assertDispatchedTimes(QueryBypassed::class, 4);
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'explicit_without_cache',
        );
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'transaction_active',
        );
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'write_pdo',
        );
        $this->assertCount(
            2,
            Event::dispatched(
                QueryBypassed::class,
                fn(QueryBypassed $event): bool => $event->reason === 'write_pdo',
            ),
        );
    }

    public function test_exists_uses_the_same_bypass_decision_and_reason(): void
    {
        Event::fake([QueryBypassed::class]);

        $this->assertTrue(
            RawPost::query()->toBase()->where('id', $this->postId)->withoutCache()->exists(),
        );

        Event::assertDispatchedTimes(QueryBypassed::class, 1);
        Event::assertDispatched(
            QueryBypassed::class,
            fn(QueryBypassed $event): bool => $event->reason === 'explicit_without_cache',
        );
    }

    public function test_corrupt_result_payload_self_heals_as_a_miss(): void
    {
        $query = fn() => RawPost::query()->toBase()
            ->where('id', $this->postId)
            ->select('title')
            ->get();
        $query();
        $key = $this->cacheQueryKeysWithField('r')[0] ?? null;

        $this->assertIsString($key);
        $this->cacheStore()->writeHashField($key, 'r', 'corrupt');
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
        RawPost::query()->toBase()->orderBy('id')->get();
        $rowKey = $this->cacheKeysMatching(':r:g')[0] ?? null;

        $this->assertIsString($rowKey);
        $this->cacheStore()->delete($rowKey);
        Event::fake([QueryCacheMiss::class]);

        RawPost::query()->toBase()->orderBy('id')->get();

        Event::assertNotDispatched(
            QueryCacheMiss::class,
            fn(QueryCacheMiss $event): bool => $event->reason === 'corrupt_payload',
        );
    }
}
