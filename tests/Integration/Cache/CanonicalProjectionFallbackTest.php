<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class CanonicalProjectionFallbackTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);

        foreach ([
            [1, 'One', 10, true],
            [2, 'Two', 20, true],
            [3, 'Three', 30, false],
            [4, 'Four', 40, true],
        ] as [$id, $title, $views, $published]) {
            DB::table('posts')->insert([
                'id' => $id,
                'title' => $title,
                'views' => $views,
                'published' => $published,
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    public function test_plain_projection_reuses_wildcard_membership_without_sql(): void
    {
        DB::table('posts')
            ->where('published', true)
            ->orderByDesc('id')
            ->limit(2)
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts')
            ->where('published', true)
            ->orderByDesc('id')
            ->limit(2)
            ->select(['id', 'title'])
            ->get();
        DB::disableQueryLog();

        $this->assertSame([
            ['id' => 4, 'title' => 'Four'],
            ['id' => 2, 'title' => 'Two'],
        ], $rows->map(fn(object $row): array => (array) $row)->all());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_projection_fallback_promotes_compact_result_payload(): void
    {
        $wildcard = DB::table('posts')
            ->where('published', true)
            ->orderBy('id');
        $wildcard->get();
        $this->deleteResultOverlays();

        $projected = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title'])
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $first = $projected();
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));

        $this->cacheStore()->delete([
            ...$this->cacheKeysMatching(':m:v'),
            ...$this->cacheKeysMatching(':r:g'),
        ]);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $second = $projected();
        DB::disableQueryLog();

        $this->assertSame(
            $first->map(fn(object $row): array => (array) $row)->all(),
            $second->map(fn(object $row): array => (array) $row)->all(),
        );
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_corrupt_projected_result_falls_back_to_canonical_and_rebuilds(): void
    {
        DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->get();
        $this->deleteResultOverlays();

        $projected = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title'])
            ->get();

        $expected = $projected()->pluck('id')->all();
        $resultKey = $this->cacheKeysMatching(':e:v')[0];
        $this->cacheStore()->setRaw($resultKey, 'corrupt', 60);
        Event::fake([QueryCacheRepaired::class]);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $projected()->pluck('id')->all();
        DB::disableQueryLog();

        $this->assertSame($expected, $actual);
        $this->assertSame([], DB::getQueryLog());
        $payload = $this->cacheStore()->getRaw($resultKey);
        $this->assertIsString($payload);
        $this->assertNotSame('corrupt', $payload);
        Event::assertDispatched(
            QueryCacheRepaired::class,
            static fn(QueryCacheRepaired $event): bool => $event->reason === 'result_overlay_rebuilt',
        );
    }

    public function test_where_in_projection_reuses_the_same_canonical_membership(): void
    {
        DB::table('posts')
            ->whereIn('id', [1, 2, 4])
            ->orderByDesc('id')
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts')
            ->whereIn('id', [1, 2, 4])
            ->orderByDesc('id')
            ->select(['id', 'title'])
            ->get();
        DB::disableQueryLog();

        $this->assertSame([4, 2, 1], $rows->pluck('id')->all());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_projection_fallback_preserves_membership_order_limit_and_offset(): void
    {
        DB::table('posts')
            ->where('published', true)
            ->orderBy('views')
            ->offset(1)
            ->limit(2)
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts')
            ->where('published', true)
            ->orderBy('views')
            ->offset(1)
            ->limit(2)
            ->select(['title', 'views'])
            ->get();
        DB::disableQueryLog();

        $this->assertSame([
            ['title' => 'Two', 'views' => 20],
            ['title' => 'Four', 'views' => 40],
        ], $rows->map(fn(object $row): array => (array) $row)->all());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_qualified_wildcard_and_projection_share_normalized_membership_identity(): void
    {
        DB::table('posts as p')
            ->where('p.published', true)
            ->orderBy('p.id')
            ->select('p.*')
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts as p')
            ->where('p.published', true)
            ->orderBy('p.id')
            ->select(['p.id', 'p.title'])
            ->get();
        DB::disableQueryLog();

        $this->assertSame([1, 2, 4], $rows->pluck('id')->all());
        $this->assertSame(['One', 'Two', 'Four'], $rows->pluck('title')->all());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_eloquent_projection_reuses_canonical_membership(): void
    {
        Post::query()
            ->where('published', true)
            ->orderBy('id')
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $posts = Post::query()
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title'])
            ->get();
        DB::disableQueryLog();

        $this->assertSame([1, 2, 4], $posts->modelKeys());
        $this->assertSame(
            [['id' => 1, 'title' => 'One'], ['id' => 2, 'title' => 'Two'], ['id' => 4, 'title' => 'Four']],
            $posts->map(fn(Post $post): array => $post->getAttributes())->all(),
        );
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_empty_canonical_membership_is_a_projection_hit(): void
    {
        DB::table('posts')->where('views', '>', 1000)->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts')
            ->where('views', '>', 1000)
            ->select(['id', 'title'])
            ->get();
        DB::disableQueryLog();

        $this->assertSame([], $rows->all());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_result_payload_wins_before_canonical_membership_fallback(): void
    {
        $projected = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title'])
            ->get();

        $projected();
        DB::table('posts')->where('published', true)->orderBy('id')->get();
        $this->cacheStore()->delete($this->rowKeyFor(2));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = $projected();
        DB::disableQueryLog();

        $this->assertSame([1, 2, 4], $rows->pluck('id')->all());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_missing_canonical_row_declines_projection_fallback_without_repair(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id');

        $query()->get();
        $rowKey = $this->rowKeyFor(2);
        $this->cacheStore()->delete($rowKey);
        Event::fake([QueryCacheRepaired::class]);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = $query()->select(['id', 'title'])->get();
        DB::disableQueryLog();

        $this->assertSame([1, 2, 4], $rows->pluck('id')->all());
        $this->assertCount(1, DB::getQueryLog());
        $this->assertNull($this->cacheStore()->getRaw($rowKey));
        Event::assertNotDispatched(QueryCacheRepaired::class);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame([1, 2, 4], $query()->select(['id', 'title'])->get()->pluck('id')->all());
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_raw_or_aliased_projection_does_not_use_canonical_membership(): void
    {
        DB::table('posts')->where('published', true)->orderBy('id')->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->selectRaw('id, upper(title) as heading')
            ->get();
        DB::disableQueryLog();

        $this->assertSame(['ONE', 'TWO', 'FOUR'], $rows->pluck('heading')->all());
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_missing_selected_column_preserves_native_error_without_deleting_valid_rows(): void
    {
        DB::table('posts')->where('published', true)->orderBy('id')->get();
        $rowKey = $this->rowKeyFor(1);
        $this->assertNotNull($this->cacheStore()->getRaw($rowKey));

        try {
            DB::table('posts')
                ->where('published', true)
                ->orderBy('id')
                ->select(['posts.id', 'posts.missing_column'])
                ->get();
            $this->fail('Expected the database to reject the missing column.');
        } catch (QueryException) {
            $this->addToAssertionCount(1);
        }

        $this->assertNotNull($this->cacheStore()->getRaw($rowKey));
    }

    public function test_projection_membership_respects_tag_namespace(): void
    {
        DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->tag('homepage')
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title'])
            ->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_projection_fallback_reports_a_hit_without_a_miss(): void
    {
        DB::table('posts')->where('published', true)->orderBy('id')->get();
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title'])
            ->get();

        Event::assertDispatched(
            QueryCacheHit::class,
            static fn(QueryCacheHit $event): bool => $event->reason === 'canonical_projection_fallback',
        );
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    private function rowKeyFor(int $id): string
    {
        foreach ($this->cacheKeysMatching(':r:g') as $key) {
            if (str_ends_with($key, ':i:' . $id)) {
                return $key;
            }
        }

        throw new \RuntimeException("Canonical row key for [{$id}] was not found.");
    }
}
