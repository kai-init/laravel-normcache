<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedAuthor;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;
use ReflectionProperty;

class CacheableBuilderTest extends TestCase
{
    public function test_get_writes_query_cache_key(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotEmpty($this->redisKeys('test:query:*'));
    }

    public function test_get_returns_same_results_as_uncached(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $cached = Author::all()->pluck('name')->sort()->values();
        $live = UncachedAuthor::all()->pluck('name')->sort()->values();

        $this->assertEquals($live, $cached);
    }

    public function test_without_cache_writes_no_query_keys(): void
    {
        Author::create(['name' => 'Alice']);
        Author::withoutCache()->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_uncached_get_accepts_string_columns(): void
    {
        Author::create(['name' => 'Alice']);

        $authors = Author::withoutCache()->get('id');

        $this->assertCount(1, $authors);
        $this->assertSame(['id'], array_keys($authors->first()->getAttributes()));
    }

    public function test_remember_uses_custom_ttl(): void
    {
        Author::create(['name' => 'Alice']);
        Author::query()->remember(9999)->get();

        $queryKey = collect($this->redisKeys('test:query:*'))->first();

        $this->assertNotNull($queryKey);
        $this->assertGreaterThan(9000, Redis::connection('model-cache-test')->ttl($queryKey));
    }

    public function test_query_with_join_bypasses_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_query_with_group_by_bypasses_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::query()->groupBy('name')->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_query_from_subquery_bypasses_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::fromSub(Author::query()->select('id', 'name'), 'authors')->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_query_with_raw_select_expression_bypasses_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::query()->selectRaw('id, name, 1 + 1 as computed')->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_subquery_where_has_reflects_related_model_writes(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id, 'published' => true]);

        $warm = Author::whereHas('posts', fn($q) => $q->where('published', true))->get();
        $this->assertCount(1, $warm);

        $post->update(['published' => false]);

        $live = UncachedAuthor::whereHas('posts', fn($q) => $q->where('published', true))->get();
        $this->assertCount(0, $live);

        $cached = Author::whereHas('posts', fn($q) => $q->where('published', true))->get();
        $this->assertCount(0, $cached);
    }

    public function test_select_specific_columns_filters_returned_attributes(): void
    {
        Author::create(['name' => 'Alice', 'country_id' => null]);

        $authors = Author::select(['id', 'name'])->get();

        /** @var Author $author **/
        foreach ($authors as $author) {
            $this->assertArrayHasKey('id', $author->getAttributes());
            $this->assertArrayHasKey('name', $author->getAttributes());
            $this->assertArrayNotHasKey('country_id', $author->getAttributes());
        }
    }

    public function test_bulk_update_invalidates_version(): void
    {
        Author::create(['name' => 'Alice']);
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::where('name', 'Alice')->update(['name' => 'Alicia']);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_bulk_delete_invalidates_version(): void
    {
        Author::create(['name' => 'Alice']);
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::where('name', 'Alice')->delete();

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_cache_aggregates_with_count_returns_correct_value(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);
        Post::create(['title' => 'Post 2', 'author_id' => $author->id]);

        $first = Author::withCount('posts')->get()->firstWhere('id', $author->id);
        $second = Author::withCount('posts')->get()->firstWhere('id', $author->id);

        $this->assertSame(2, $first->posts_count);
        $this->assertSame(2, $second->posts_count);
    }

    public function test_cache_aggregates_with_count_respects_runtime_global_scope_state(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'views' => 10]);
        Post::create(['title' => 'B', 'author_id' => $author->id, 'views' => 20]);

        $threshold = 0;
        $enabled = true;
        Post::addGlobalScope('viewsScope', function ($query) use (&$threshold, &$enabled) {
            if ($enabled) {
                $query->where('views', '>=', $threshold);
            }
        });

        try {
            $threshold = 0;
            $first = Author::withCount('posts')->get()->firstWhere('id', $author->id);
            $this->assertSame(2, (int) $first->posts_count);

            $threshold = 15;
            $second = Author::withCount('posts')->get()->firstWhere('id', $author->id);
            $this->assertSame(1, (int) $second->posts_count);
        } finally {
            $enabled = false;
            $this->clearGlobalScope(Post::class, 'viewsScope');
        }
    }

    public function test_cache_aggregates_with_sum_returns_correct_value(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'views' => 10, 'author_id' => $author->id]);
        Post::create(['title' => 'Post 2', 'views' => 20, 'author_id' => $author->id]);

        $first = Author::withSum('posts', 'views')->get()->firstWhere('id', $author->id);
        $second = Author::withSum('posts', 'views')->get()->firstWhere('id', $author->id);

        $this->assertEquals(30, $first->posts_sum_views);
        $this->assertEquals(30, $second->posts_sum_views);
    }

    public function test_cache_aggregates_with_avg_returns_correct_value(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'views' => 10, 'author_id' => $author->id]);
        Post::create(['title' => 'Post 2', 'views' => 20, 'author_id' => $author->id]);

        $first = Author::withAvg('posts', 'views')->get()->firstWhere('id', $author->id);
        $second = Author::withAvg('posts', 'views')->get()->firstWhere('id', $author->id);

        $this->assertEquals(15, $first->posts_avg_views);
        $this->assertEquals(15, $second->posts_avg_views);
    }

    public function test_cache_aggregates_with_min_returns_correct_value(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'views' => 10, 'author_id' => $author->id]);
        Post::create(['title' => 'Post 2', 'views' => 20, 'author_id' => $author->id]);

        $first = Author::withMin('posts', 'views')->get()->firstWhere('id', $author->id);
        $second = Author::withMin('posts', 'views')->get()->firstWhere('id', $author->id);

        $this->assertEquals(10, $first->posts_min_views);
        $this->assertEquals(10, $second->posts_min_views);
    }

    public function test_cache_aggregates_with_max_returns_correct_value(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'views' => 10, 'author_id' => $author->id]);
        Post::create(['title' => 'Post 2', 'views' => 20, 'author_id' => $author->id]);

        $first = Author::withMax('posts', 'views')->get()->firstWhere('id', $author->id);
        $second = Author::withMax('posts', 'views')->get()->firstWhere('id', $author->id);

        $this->assertEquals(20, $first->posts_max_views);
        $this->assertEquals(20, $second->posts_max_views);
    }

    public function test_cache_aggregates_with_exists_returns_correct_value(): void
    {
        $authorWith = Author::create(['name' => 'Alice']);
        $authorWithout = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Post 1', 'author_id' => $authorWith->id]);

        $first = Author::withExists('posts')->get();
        $second = Author::withExists('posts')->get();

        $this->assertTrue((bool) $first->firstWhere('id', $authorWith->id)->posts_exists);
        $this->assertFalse((bool) $first->firstWhere('id', $authorWithout->id)->posts_exists);
        $this->assertTrue((bool) $second->firstWhere('id', $authorWith->id)->posts_exists);
        $this->assertFalse((bool) $second->firstWhere('id', $authorWithout->id)->posts_exists);
    }

    public function test_aggregate_cache_keys_can_accumulate_after_version_bumps(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        Author::withCount('posts')->get();

        $aggKeysAfterFirstLoad = $this->redisKeys('test:agg:*');
        $this->assertCount(1, $aggKeysAfterFirstLoad);

        Post::create(['title' => 'Post 2', 'author_id' => $author->id]);

        Author::withCount('posts')->get();

        $aggKeysAfterSecondLoad = $this->redisKeys('test:agg:*');

        $this->assertCount(2, $aggKeysAfterSecondLoad);
    }

    public function test_flush_model_can_reuse_existing_aggregate_keys(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        Author::withCount('posts')->get();

        $keysBeforeFlush = $this->redisKeys('test:agg:*');
        $this->assertCount(1, $keysBeforeFlush);

        NormCache::forceFlushModel(Author::class);

        $this->assertSame(1, Author::withCount('posts')->first()->posts_count);
        $this->assertCount(1, $this->redisKeys('test:agg:*'));
    }

    public function test_with_count_on_non_cacheable_relation_falls_through_to_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        UncachedPost::create(['title' => 'Post 1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'Post 2', 'author_id' => $author->id]);

        $result = Author::withCount('uncachedPosts')->get()->firstWhere('id', $author->id);

        $this->assertSame(2, (int) $result->uncached_posts_count);
        $this->assertEmpty($this->redisKeys('test:agg:*'));
    }

    public function test_eager_loaded_relations_are_returned(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $authors = Author::with('posts')->get();
        $found = $authors->firstWhere('id', $author->id);

        $this->assertTrue($found->relationLoaded('posts'));
        $this->assertCount(1, $found->posts);
    }

    public function test_belongs_to_eager_loads_from_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::with('author')->get();
        $post = Post::with('author')->first();

        $this->assertTrue($post->relationLoaded('author'));
        $this->assertSame('Alice', $post->author->name);
    }

    public function test_belongs_to_warm_hit_runs_after_query_callbacks(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $count = 0;

        Post::with(['author' => function ($query) use (&$count) {
            $query->afterQuery(function () use (&$count) {
                $count++;
            });
        }])->get();

        Post::with(['author' => function ($query) use (&$count) {
            $query->afterQuery(function () use (&$count) {
                $count++;
            });
        }])->get();

        $this->assertSame(2, $count);
    }

    public function test_belongs_to_eager_load_respects_join_only_global_scope_on_warm_hit(): void
    {
        Author::create(['name' => 'Alice', 'country_id' => null]);
        Post::create(['title' => 'Hello', 'author_id' => 1]);

        Post::with('author')->get();

        $enabled = true;
        Author::addGlobalScope('joinCountry', function ($query) use (&$enabled) {
            if ($enabled) {
                $query->join('countries', 'authors.country_id', '=', 'countries.id');
            }
        });

        try {
            $post = Post::with('author')->find(1);

            $this->assertNull($post->author);
        } finally {
            $enabled = false;
            $this->clearGlobalScope(Author::class, 'joinCountry');
        }
    }

    public function test_belongs_to_eager_load_with_selected_columns_uses_normal_relation_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::with('author:id')->get();
        $post = Post::with('author:id')->first();

        $this->assertTrue($post->relationLoaded('author'));
        $this->assertSame($author->id, $post->author->id);
        $this->assertArrayNotHasKey('name', $post->author->getAttributes());
    }

    public function test_nested_belongs_to_eager_load_uses_normal_relation_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $post = Post::with('author.posts')->first();

        $this->assertTrue($post->relationLoaded('author'));
        $this->assertTrue($post->author->relationLoaded('posts'));
        $this->assertCount(1, $post->author->posts);
    }

    public function test_in_random_order_bypasses_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::inRandomOrder()->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_primary_key_query_with_limit_uses_model_cache_without_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $authors = Author::whereKey($author->id)->limit(1)->get();

        $this->assertCount(1, $authors);
        $this->assertSame('Alice', $authors->first()->name);
        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_single_primary_key_query_with_order_uses_model_cache_without_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $authors = Author::whereKey($author->id)->orderBy('name')->get();

        $this->assertCount(1, $authors);
        $this->assertSame('Alice', $authors->first()->name);
        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_primary_key_query_with_zero_limit_returns_empty_without_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $authors = Author::whereKey($author->id)->limit(0)->get();

        $this->assertCount(0, $authors);
        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_increment_invalidates_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::where('id', $author->id)->increment('id', 0);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_decrement_invalidates_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::where('id', $author->id)->decrement('id', 0);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_query_inside_transaction_bypasses_cache(): void
    {
        Author::create(['name' => 'Alice']);

        DB::transaction(function () {
            Author::all();
        });

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_refresh_issues_a_db_query_not_a_cache_read(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::find($author->id);

        DB::enableQueryLog();
        $author->refresh();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotEmpty($queries);
    }

    public function test_truncate_flushes_model_cache_and_increments_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->truncate(); // returns void

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_paginate_fires_query_cache_miss_on_first_call(): void
    {
        Event::fake([QueryCacheMiss::class]);

        Author::create(['name' => 'Alice']);
        Author::paginate(10);

        Event::assertDispatched(QueryCacheMiss::class, function (QueryCacheMiss $e) {
            return $e->modelClass === Author::class;
        });
    }

    public function test_paginate_fires_query_cache_hit_on_second_call(): void
    {
        Author::create(['name' => 'Alice']);
        Author::paginate(10);

        Event::fake([QueryCacheHit::class]);

        Author::paginate(10);

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return $e->modelClass === Author::class;
        });
    }

    public function test_paginate_fires_query_bypassed_for_bypassed_query(): void
    {
        Event::fake([QueryBypassed::class]);

        Author::create(['name' => 'Alice']);
        Author::query()->groupBy('name')->paginate(10);

        Event::assertDispatched(QueryBypassed::class, function (QueryBypassed $e) {
            return $e->modelClass === Author::class
                && isset($e->reasons['normalization']);
        });
    }

    public function test_paginate_caches_count_and_returns_correct_results(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::create(['name' => 'Carol']);

        $first = Author::paginate(2, ['*'], 'page', 1);

        $this->assertNotEmpty($this->redisKeys('test:count:*'));
        $this->assertSame(3, $first->total());
        $this->assertCount(2, $first->items());

        // Second call should use cached count
        $second = Author::paginate(2, ['*'], 'page', 2);

        $this->assertSame(3, $second->total());
        $this->assertCount(1, $second->items());
    }

    public function test_paginate_count_cache_is_select_independent(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        Author::query()->paginate(10);
        Author::query()->select('name')->paginate(10);

        $this->assertCount(1, $this->redisKeys('test:count:*'));
    }

    public function test_raw_builder_insert_invalidates_version(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->insert(['name' => 'Alice', 'created_at' => now(), 'updated_at' => now()]);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_raw_builder_insert_is_reflected_in_subsequent_queries(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        Author::query()->insert(['name' => 'Bob', 'created_at' => now(), 'updated_at' => now()]);

        $names = Author::all()->pluck('name');

        $this->assertContains('Bob', $names);
    }

    public function test_updating_related_model_busts_aggregate_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        Author::withCount('posts')->get();

        DB::table('posts')->insert([
            'title' => 'Post 2',
            'author_id' => $author->id,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $post->update(['title' => 'Updated']);
        $result = Author::withCount('posts')->get()
            ->firstWhere('id', $author->id);

        $this->assertSame(2, $result->posts_count);
    }

    public function test_bulk_delete_with_rows_affected_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        Post::create(['title' => 'P2', 'author_id' => $author->id]);

        Post::all(); // warm query cache
        $versionBefore = NormCache::currentVersion(Post::class);

        Post::where('author_id', $author->id)->delete();

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Post::class));

        $posts = Post::all();
        $this->assertCount(0, $posts);
    }

    public function test_bulk_update_affecting_zero_rows_does_not_invalidate_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::all();
        $versionBefore = NormCache::currentVersion(Author::class);

        $affected = Author::where('id', 99999)->update(['name' => 'Ghost']);

        $this->assertSame(0, $affected);
        $this->assertSame($versionBefore, NormCache::currentVersion(Author::class));
    }

    // -------------------------------------------------------------------------
    // explain() + QueryBypassed event
    // -------------------------------------------------------------------------

    public function test_explain_returns_cached_for_simple_query(): void
    {
        $this->assertSame('cached', Author::query()->explain());
    }

    public function test_explain_groups_where_has_as_dependency(): void
    {
        $result = Author::whereHas('posts')->explain();

        $this->assertStringContainsString("can't infer cache dependency", $result);
        $this->assertStringContainsString('subquery WHERE', $result);
        $this->assertStringStartsWith('not cached', $result);
    }

    public function test_explain_groups_join_as_normalization(): void
    {
        $result = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->explain();

        $this->assertStringContainsString("can't be normalized", $result);
        $this->assertStringContainsString('JOIN', $result);
    }

    public function test_explain_groups_non_standard_from_as_normalization(): void
    {
        $result = Author::fromSub(Author::query()->select('id', 'name'), 'authors')
            ->explain();

        $this->assertStringContainsString("can't be normalized", $result);
        $this->assertStringContainsString('non-standard FROM', $result);
    }

    public function test_explain_groups_group_by_as_normalization(): void
    {
        $result = Author::query()->groupBy('name')->explain();

        $this->assertStringContainsString("can't be normalized", $result);
        $this->assertStringContainsString('GROUP BY', $result);
    }

    public function test_explain_groups_explicit_skip_as_opted_out(): void
    {
        $result = Author::withoutCache()->explain();

        $this->assertStringContainsString('explicitly disabled', $result);
        $this->assertStringContainsString('withoutCache()', $result);
    }

    public function test_explain_shows_all_categories_when_multiple_apply(): void
    {
        $result = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->groupBy('authors.id')
            ->explain();

        $this->assertStringContainsString("can't be normalized", $result);
        $this->assertStringContainsString('JOIN', $result);
        $this->assertStringContainsString('GROUP BY', $result);
    }

    public function test_get_fires_query_bypassed_event_with_dependency_category_for_where_has(): void
    {
        Event::fake([QueryBypassed::class]);

        Author::create(['name' => 'Alice']);
        Author::whereHas('posts')->get();

        Event::assertDispatched(QueryBypassed::class, function (QueryBypassed $e) {
            return $e->modelClass === Author::class
                && isset($e->reasons['dependency'])
                && collect($e->reasons['dependency'])->contains(fn($r) => str_contains($r, 'subquery WHERE'));
        });
    }

    public function test_get_fires_query_bypassed_event_with_normalization_category_for_group_by(): void
    {
        Event::fake([QueryBypassed::class]);

        Author::create(['name' => 'Alice']);
        Author::query()->groupBy('name')->get();

        Event::assertDispatched(QueryBypassed::class, function (QueryBypassed $e) {
            return $e->modelClass === Author::class
                && isset($e->reasons['normalization'])
                && collect($e->reasons['normalization'])->contains(fn($r) => str_contains($r, 'GROUP BY'));
        });
    }

    public function test_get_fires_query_bypassed_event_with_normalization_for_calculated_column(): void
    {
        Event::fake([QueryBypassed::class]);

        Author::create(['name' => 'Alice']);
        Author::query()->selectRaw('id, name, 1 + 1 as computed')->get();

        Event::assertDispatched(QueryBypassed::class, function (QueryBypassed $e) {
            return $e->modelClass === Author::class
                && isset($e->reasons['normalization'])
                && collect($e->reasons['normalization'])->contains(fn($r) => str_contains($r, 'calculated'));
        });
    }

    public function test_get_does_not_fire_query_bypassed_event_for_pure_query(): void
    {
        Event::fake([QueryBypassed::class]);

        Author::create(['name' => 'Alice']);
        Author::all();

        Event::assertNotDispatched(QueryBypassed::class);
    }

    public function test_warm_hit_runs_after_query_callbacks(): void
    {
        Author::create(['name' => 'Alice']);

        $count = 0;

        Author::query()->afterQuery(function () use (&$count) {
            $count++;
        })->get();

        Author::query()->afterQuery(function () use (&$count) {
            $count++;
        })->get();

        $this->assertSame(2, $count);
    }

    private function clearGlobalScope(string $modelClass, string $name): void
    {
        $prop = new ReflectionProperty(Model::class, 'globalScopes');
        $scopes = $prop->getValue();
        unset($scopes[$modelClass][$name]);
        $prop->setValue(null, $scopes);
    }
}
