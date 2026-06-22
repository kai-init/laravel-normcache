<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Query\Expression;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\VersionTracker;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\InstrumentedPost;
use NormCache\Tests\Fixtures\Models\NewFromBuilderOverridingPost;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class ModelHydratorClosureColdHydrationTest extends TestCase
{
    protected function tearDown(): void
    {
        Post::flushEventListeners();

        parent::tearDown();
    }

    private function makeHydrator(): ModelHydrator
    {
        $store = $this->cacheManager()->getStore();
        $keys = new CacheKeyBuilder;
        $versions = new VersionTracker($store, $keys);

        return new ModelHydrator($store, $keys, $versions, 3600, true, 5, 200);
    }

    private function invokeGuard(ModelHydrator $hydrator, EloquentBuilder $query): bool
    {
        $method = new \ReflectionMethod($hydrator, 'overridesNewFromBuilder');

        return !$method->invoke($hydrator, $query->getModel());
    }

    public function test_guard_allows_simple_model_table_lookup(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache();

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_allows_query_with_joins(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache()
            ->join('authors', 'authors.id', '=', 'posts.author_id');

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_allows_query_with_unions(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache();
        $query->getQuery()->unions = [['query' => Post::query()->getQuery(), 'all' => false]];

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_allows_query_with_groups_and_havings(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache()
            ->groupBy('author_id')
            ->havingRaw('count(*) > 0');

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_allows_query_with_aggregate(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache();
        $query->getQuery()->aggregate = ['function' => 'count', 'columns' => ['*']];

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_allows_query_with_custom_select_columns(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache()->select('id', 'title');

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_allows_non_canonical_from(): void
    {
        $hydrator = $this->makeHydrator();
        $query = Post::query()->withoutCache();
        $query->getQuery()->from = new Expression('(select * from posts) as p');

        $this->assertTrue($this->invokeGuard($hydrator, $query));
    }

    public function test_guard_rejects_model_overriding_new_from_builder(): void
    {
        $hydrator = $this->makeHydrator();
        $query = NewFromBuilderOverridingPost::query()->withoutCache();

        $this->assertFalse($this->invokeGuard($hydrator, $query));
    }

    public function test_simple_cold_miss_uses_closure_hydration_without_calling_set_raw_attributes(): void
    {
        $author = Author::create(['name' => 'Dana']);
        $post = InstrumentedPost::create(['title' => 'Hello', 'author_id' => $author->id]);
        InstrumentedPost::$setRawAttributesCalls = 0;
        $this->evictModelCache(InstrumentedPost::class, $post->id);

        $manager = $this->buildManager();
        $models = $manager->getModels([$post->id], InstrumentedPost::class);

        $this->assertCount(1, $models);
        $this->assertSame('Hello', $models[0]->title);
        $this->assertSame(0, InstrumentedPost::$setRawAttributesCalls, 'Closure hydration must bypass setRawAttributes()/newFromBuilder()');
        $this->assertTrue($models[0]->exists);
    }

    public function test_custom_new_from_builder_override_uses_eloquent_fallback(): void
    {
        $author = Author::create(['name' => 'Eli']);
        $post = NewFromBuilderOverridingPost::create(['title' => 'Custom', 'author_id' => $author->id]);
        NewFromBuilderOverridingPost::$newFromBuilderCalls = 0;
        $this->evictModelCache(NewFromBuilderOverridingPost::class, $post->id);

        $manager = $this->buildManager();
        $models = $manager->getModels([$post->id], NewFromBuilderOverridingPost::class);

        $this->assertCount(1, $models);
        $this->assertSame('Custom', $models[0]->title);
        $this->assertSame(1, NewFromBuilderOverridingPost::$newFromBuilderCalls, 'Models overriding newFromBuilder() must use the Eloquent fallback');
    }

    public function test_projection_caches_full_attributes_and_returns_projected_model(): void
    {
        $author = Author::create(['name' => 'Fay']);
        $post = Post::create(['title' => 'Projected', 'author_id' => $author->id, 'views' => 5, 'published' => true]);
        $this->evictModelCache(Post::class, $post->id);

        $projected = Post::select('id', 'title')->whereKey($post->id)->get();

        $this->assertSame(['Projected'], $projected->pluck('title')->all());
        $this->assertArrayNotHasKey('views', $projected->first()->getAttributes());

        $cached = $this->modelCacheEntry(Post::class, $post->id);
        $this->assertSame(5, $cached['views']);
        $this->assertSame($post->author_id, $cached['author_id']);

        DB::enableQueryLog();
        $full = Post::whereKey($post->id)->get()->first();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame(5, $full->views);
        $this->assertCount(0, $queries, 'full payload should already be cached from the projected miss');
    }

    public function test_cold_miss_closure_hydration_fires_retrieved_event_regardless_of_fire_retrieved_config(): void
    {
        $author = Author::create(['name' => 'Gail']);
        $post = Post::create(['title' => 'Retrieved', 'author_id' => $author->id]);
        $this->evictModelCache(Post::class, $post->id);

        $calls = 0;
        Post::retrieved(function () use (&$calls) {
            $calls++;
        });

        $manager = $this->buildManager(fireRetrieved: false);
        $models = $manager->getModels([$post->id], Post::class);

        $this->assertCount(1, $models);
        $this->assertSame(1, $calls, 'Cold-miss closure hydration must fire retrieved exactly once, matching the previous Eloquent-hydration behavior');
    }

    public function test_connection_name_matches_native_eloquent_after_cold_miss(): void
    {
        $author = Author::create(['name' => 'Hank']);
        $post = InstrumentedPost::create(['title' => 'Conn', 'author_id' => $author->id]);
        $this->evictModelCache(InstrumentedPost::class, $post->id);

        $manager = $this->buildManager();
        $models = $manager->getModels([$post->id], InstrumentedPost::class);

        $native = InstrumentedPost::find($post->id);

        $this->assertSame($native->getConnectionName(), $models[0]->getConnectionName());
        $this->assertSame('testing', $models[0]->getConnectionName());
    }

    public function test_eager_loading_happens_only_during_finalization_not_inside_optimized_fetch(): void
    {
        $author = Author::create(['name' => 'Ivy']);
        $post = Post::create(['title' => 'WithComments', 'author_id' => $author->id]);
        Comment::create(['body' => 'Nice post', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        $this->evictModelCache(Post::class, $post->id);

        $this->contract(
            cached: fn() => Post::with('comments')->whereKey($post->id)->get(),
            native: fn() => Post::withoutCache()->with('comments')->whereKey($post->id)->get(),
        );

        $loaded = Post::with('comments')->whereKey($post->id)->get()->first();
        $this->assertTrue($loaded->relationLoaded('comments'));
        $this->assertCount(1, $loaded->comments);
    }

    public function test_after_query_callback_runs_exactly_once_per_call_on_cold_miss(): void
    {
        $author = Author::create(['name' => 'Jack']);
        $post = Post::create(['title' => 'Callback', 'author_id' => $author->id]);
        $this->evictModelCache(Post::class, $post->id);

        $calls = 0;
        $query = function () use ($post, &$calls) {
            return Post::whereKey($post->id)
                ->afterQuery(function ($posts) use (&$calls) {
                    $calls++;

                    return $posts;
                })
                ->get();
        };

        $cold = $query();
        $warm = $query();

        $this->assertSame(['Callback'], $cold->pluck('title')->all());
        $this->assertSame(['Callback'], $warm->pluck('title')->all());
        $this->assertSame(2, $calls, 'callback should run exactly once per get() call, not once per fetched row');
    }

    public function test_soft_deleted_row_is_not_cached_as_active_model_payload(): void
    {
        $author = Author::create(['name' => 'Kim']);
        $post = Post::create(['title' => 'Trashed', 'author_id' => $author->id]);
        $post->delete();

        $this->evictModelCache(Post::class, $post->id);

        $this->contract(
            cached: fn() => Post::withTrashed()->whereKey($post->id)->get(),
            native: fn() => Post::withoutCache()->withTrashed()->whereKey($post->id)->get(),
        );

        $this->assertNull($this->modelCacheEntry(Post::class, $post->id), 'soft-deleted rows must not be cached as an active model payload');
    }

    public function test_query_with_join_and_explicit_select_returns_correct_models(): void
    {
        $author = Author::create(['name' => 'Lyle']);
        $post = Post::create(['title' => 'Joined', 'author_id' => $author->id]);
        $this->evictModelCache(Post::class, $post->id);

        $this->contract(
            cached: fn() => Post::query()->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.*')
                ->whereKey($post->id)
                ->get(),
            native: fn() => Post::withoutCache()->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.*')
                ->whereKey($post->id)
                ->get(),
        );
    }

    public function test_query_with_join_and_explicit_select_uses_closure_hydration(): void
    {
        $author = Author::create(['name' => 'Nora']);
        $post = InstrumentedPost::create(['title' => 'JoinedInstrumented', 'author_id' => $author->id]);
        $this->evictModelCache(InstrumentedPost::class, $post->id);
        InstrumentedPost::$setRawAttributesCalls = 0;

        $models = InstrumentedPost::query()
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->select('posts.*')
            ->whereKey($post->id)
            ->get();

        $this->assertSame('JoinedInstrumented', $models->first()->title);
        $this->assertSame(0, InstrumentedPost::$setRawAttributesCalls, 'join query with explicit select should still be hydrated through the closure path');
    }

    public function test_warm_cold_parity_for_casts_dates_json_booleans_and_accessors(): void
    {
        $author = Author::create(['name' => 'Mona']);
        Post::create(['title' => 'Parity', 'author_id' => $author->id, 'published' => true, 'metadata' => ['k' => 'v']]);

        $this->contract(
            cached: fn() => Post::where('published', true)->get(),
            native: fn() => Post::withoutCache()->where('published', true)->get(),
        );

        $post = Post::where('published', true)->first();
        $this->assertIsBool($post->published);
        $this->assertSame(['k' => 'v'], $post->metadata);
        $this->assertSame('calculated_value', $post->calculated_field);
        $this->assertInstanceOf(Carbon::class, $post->created_at);
    }
}
