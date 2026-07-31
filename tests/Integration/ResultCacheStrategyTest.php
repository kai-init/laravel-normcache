<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

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

    public function test_small_canonical_result_automatically_materializes_an_overlay(): void
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

    public function test_unlimited_small_canonical_result_automatically_materializes_an_overlay(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->get();

        $cold = $query();

        $this->assertCount(5, $cold);
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));

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

    public function test_one_row_allowance_applies_to_non_paginated_results(): void
    {
        foreach (range(1, 45) as $index) {
            DB::table('posts')->insert([
                'title' => "Extra {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $query = fn() => DB::table('posts')
            ->orderBy('id')
            ->get();

        $this->assertCount(51, $query());
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(51, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_query_builder_simple_pagination_uses_the_one_row_lookahead_allowance(): void
    {
        foreach (range(1, 45) as $index) {
            DB::table('posts')->insert([
                'title' => "Extra {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $query = fn() => DB::table('posts')
            ->orderBy('id')
            ->simplePaginate(50);

        $cold = $query();

        $this->assertCount(50, $cold->items());
        $this->assertTrue($cold->hasMorePages());
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $query();
        DB::disableQueryLog();

        $this->assertCount(50, $warm->items());
        $this->assertTrue($warm->hasMorePages());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_pagination_lookahead_row_still_counts_toward_the_payload_size_limit(): void
    {
        foreach (range(1, 45) as $index) {
            DB::table('posts')->insert([
                'title' => "Extra {$index}",
                'views' => $index,
                'published' => true,
                'metadata' => $index === 45
                    ? json_encode(['payload' => str_repeat('x', 64 * 1024)])
                    : null,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $query = fn() => DB::table('posts')
            ->orderBy('id')
            ->simplePaginate(50);

        $cold = $query();

        $this->assertCount(50, $cold->items());
        $this->assertTrue($cold->hasMorePages());
        $this->assertSame([], $this->cacheKeysMatching(':e:v'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $query();
        DB::disableQueryLog();

        $this->assertCount(50, $warm->items());
        $this->assertTrue($warm->hasMorePages());
        $this->assertSame([], DB::getQueryLog());
        $this->assertSame([], $this->cacheKeysMatching(':e:v'));
    }

    public function test_eloquent_forwards_use_result_cache_to_the_query_builder(): void
    {
        $query = fn() => Post::query()
            ->where('published', true)
            ->orderByDesc('views')
            ->limit(3)
            ->get();

        $cold = $query();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $query();
        DB::disableQueryLog();

        $this->assertSame($cold->modelKeys(), $warm->modelKeys());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_zero_row_limit_disables_automatic_result_overlays(): void
    {
        $originalConfig = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['auto_overlay_max_rows'] = 0;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        try {
            $query = fn() => DB::table('posts')
                ->where('published', true)
                ->orderBy('id')
                ->limit(4)
                ->get();

            $this->assertCount(4, $query());
            $this->assertSame([], $this->cacheKeysMatching(':e:v'));

            DB::flushQueryLog();
            DB::enableQueryLog();
            $this->assertCount(4, $query());
            DB::disableQueryLog();

            $this->assertSame([], DB::getQueryLog());
            $this->assertSame([], $this->cacheKeysMatching(':e:v'));
        } finally {
            $this->app->instance(CacheConfig::class, $originalConfig);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_zero_row_limit_disables_overlays_for_results_inside_the_lookahead_allowance(): void
    {
        $originalConfig = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['auto_overlay_max_rows'] = 0;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        try {
            foreach ([1, 0] as $expected) {
                Redis::connection('normcache-test')->flushdb();

                $query = fn() => DB::table('posts')
                    ->where('published', true)
                    ->where('views', $expected === 1 ? '=' : '>', $expected === 1 ? 10 : 10_000)
                    ->orderBy('id')
                    ->get();

                $this->assertCount($expected, $query());
                $this->assertSame([], $this->cacheKeysMatching(':e:v'));

                $this->assertCount($expected, $query());
                $this->assertSame([], $this->cacheKeysMatching(':e:v'));
            }
        } finally {
            $this->app->instance(CacheConfig::class, $originalConfig);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_result_larger_than_the_row_limit_plus_allowance_is_not_promoted(): void
    {
        foreach (range(1, 50) as $index) {
            DB::table('posts')->insert([
                'title' => "Extra {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(55)
            ->get();

        $this->assertCount(55, $query());
        $this->assertSame([], $this->cacheKeysMatching(':e:v'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(55, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_result_larger_than_the_payload_limit_is_not_promoted(): void
    {
        DB::table('posts')
            ->where('id', 1)
            ->update([
                'metadata' => json_encode([
                    'payload' => str_repeat('x', 64 * 1024),
                ]),
            ]);

        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
            ->get();

        $this->assertCount(4, $query());
        $this->assertSame([], $this->cacheKeysMatching(':e:v'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(4, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        $this->assertSame([], $this->cacheKeysMatching(':e:v'));
    }

    public function test_many_mid_sized_rows_under_the_payload_limit_are_still_promoted(): void
    {
        foreach (range(1, 45) as $index) {
            DB::table('posts')->insert([
                'title' => "Wide {$index}",
                'views' => $index,
                'published' => true,
                'metadata' => json_encode(['blob' => bin2hex(random_bytes(150))]),
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(45)
            ->get();

        $this->assertCount(45, $query());
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $query();
        DB::disableQueryLog();

        $this->assertCount(45, $warm);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_missing_result_overlay_falls_back_to_canonical_and_repromotes(): void
    {
        $query = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->limit(4)
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
