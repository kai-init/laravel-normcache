<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class ResultCacheStrategyTest extends TestCase
{
    private int $authorId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->authorId = (int) $author->getKey();

        foreach (range(1, 6) as $index) {
            DB::table('posts')->insert([
                'title' => "Post {$index}",
                'views' => $index * 10,
                'published' => $index !== 5,
                'metadata' => json_encode(['index' => $index]),
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    public function test_use_result_cache_materializes_result_over_canonical_storage(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->where(function ($query): void {
                $query->whereBetween('views', [10, 60])
                    ->where(function ($query): void {
                        $query->where('title', 'like', 'Post%')
                            ->orWhereNull('metadata');
                    });
            })
            ->orderByDesc('views')
            ->orderBy('id')
            ->limit(4)
            ->useResultCache()
            ->get();

        $cold = $query();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $query();
        DB::disableQueryLog();

        $this->assertSame(
            $cold->map(static fn(object $row): array => (array) $row)->all(),
            $warm->map(static fn(object $row): array => (array) $row)->all(),
        );
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_eloquent_forwards_use_result_cache_to_the_query_builder(): void
    {
        $query = fn() => Post::query()
            ->where('published', true)
            ->orderByDesc('views')
            ->limit(3)
            ->useResultCache()
            ->get();

        $cold = $query();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $query();
        DB::disableQueryLog();

        $this->assertSame($cold->modelKeys(), $warm->modelKeys());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_large_result_is_published_without_an_admission_limit(): void
    {
        $title = str_repeat('x', 4_194_304 + 1_024);
        $id = DB::table('posts')->insertGetId([
            'title' => $title,
            'views' => 0,
            'published' => true,
            'author_id' => $this->authorId,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $row = DB::table('posts')->where('id', $id)->select('title')->first();

        $this->assertSame(strlen($title), strlen((string) $row?->title));
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));
    }

    public function test_missing_result_overlay_falls_back_to_canonical_and_repromotes(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->useResultCache()
            ->get();

        $expected = $query()->pluck('id')->all();
        $resultKey = $this->cacheKeysMatching(':e:v')[0];
        $this->cacheStore()->delete($resultKey);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $query()->pluck('id')->all();
        DB::disableQueryLog();

        $this->assertSame($expected, $actual);
        $this->assertSame([], DB::getQueryLog());
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));
    }

    public function test_corrupt_result_overlay_falls_back_to_canonical_and_self_heals(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->useResultCache()
            ->get();

        $expected = $query()->pluck('id')->all();
        $resultKey = $this->cacheKeysMatching(':e:v')[0];
        $this->cacheStore()->setRaw($resultKey, 'corrupt', 60);
        Event::fake([QueryCacheRepaired::class]);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $query()->pluck('id')->all();
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

    public function test_write_invalidates_the_materialized_overlay(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->useResultCache()
            ->get();

        $before = $query();
        $id = (int) $before->first()->id;
        DB::table('posts')->where('id', $id)->update(['title' => 'Changed']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $after = $query();
        DB::disableQueryLog();

        $this->assertSame('Changed', $after->firstWhere('id', $id)->title);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_tag_flush_invalidates_the_materialized_overlay(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->tag('homepage')
            ->useResultCache()
            ->get();

        $query();
        $query();
        $this->assertTrue($this->cacheManager()->flushTag('homepage'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $query();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_dependency_version_invalidates_the_materialized_overlay(): void
    {
        $query = fn() => DB::table('posts')
            ->dependsOn(['authors'])
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->useResultCache()
            ->get();

        $query();
        $query();
        DB::table('authors')->update(['name' => 'Changed']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $query();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_query_ttl_applies_to_membership_and_result_overlay(): void
    {
        DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->ttl(30)
            ->useResultCache()
            ->get();

        $connection = Redis::connection('normcache-test');
        $membershipKey = $this->cacheKeysMatching(':m:v')[0];
        $resultKey = $this->cacheKeysMatching(':e:v')[0];
        $membershipTtl = (int) $connection->ttl($membershipKey);
        $resultTtl = (int) $connection->ttl($resultKey);

        $this->assertGreaterThan(0, $membershipTtl);
        $this->assertLessThanOrEqual(30, $membershipTtl);
        $this->assertGreaterThan(0, $resultTtl);
        $this->assertLessThanOrEqual(30, $resultTtl);
    }
}
