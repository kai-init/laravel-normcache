<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\VersionTracker;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\InstrumentedPost;
use NormCache\Tests\Fixtures\Models\NewFromBuilderOverridingPost;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class ModelHydratorColdHydrationTest extends TestCase
{
    protected function tearDown(): void
    {
        Post::flushEventListeners();

        parent::tearDown();
    }

    private function makeHydrator(): ModelHydrator
    {
        $store = $this->cacheManager()->store();
        $keys = new CacheKeyBuilder;
        $versions = new VersionTracker($store, $keys);

        return new ModelHydrator($store, $keys, $versions, 3600, true, 5, 200);
    }

    public function test_simple_cold_miss_uses_closure_hydration_without_calling_set_raw_attributes(): void
    {
        $author = Author::create(['name' => 'Dana']);
        $post = InstrumentedPost::create(['title' => 'Hello', 'author_id' => $author->id]);
        InstrumentedPost::$setRawAttributesCalls = 0;
        $this->evictModelCache(InstrumentedPost::class, $post->id);

        $manager = $this->buildManager();
        $models = $manager->hydrator()->getModels([$post->id], InstrumentedPost::class);

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
        $models = $manager->hydrator()->getModels([$post->id], NewFromBuilderOverridingPost::class);

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
        $models = $manager->hydrator()->getModels([$post->id], Post::class);

        $this->assertCount(1, $models);
        $this->assertSame(1, $calls, 'Cold-miss closure hydration must fire retrieved exactly once, matching the previous Eloquent-hydration behavior');
    }

    public function test_partial_hit_fires_retrieved_once_per_returned_model(): void
    {
        $author = Author::create(['name' => 'Half Warm']);
        $cached = Post::create(['title' => 'Cached', 'author_id' => $author->id]);
        $missing = Post::create(['title' => 'Missing', 'author_id' => $author->id]);

        $manager = $this->buildManager(fireRetrieved: true);
        $manager->hydrator()->getModels([$cached->id], Post::class);

        $retrievedIds = [];
        Post::retrieved(function (Post $post) use (&$retrievedIds): void {
            $retrievedIds[] = $post->id;
        });

        $models = $manager->hydrator()->getModels([$cached->id, $missing->id], Post::class);

        $this->assertSame([$cached->id, $missing->id], array_map(static fn(Post $post) => $post->id, $models));
        $this->assertSame(
            [$cached->id => 1, $missing->id => 1],
            array_count_values($retrievedIds),
            'The all-hit probe must not hydrate cached rows before falling back to partial-miss handling.',
        );
    }

    public function test_connection_name_matches_native_eloquent_after_cold_miss(): void
    {
        $author = Author::create(['name' => 'Hank']);
        $post = InstrumentedPost::create(['title' => 'Conn', 'author_id' => $author->id]);
        $this->evictModelCache(InstrumentedPost::class, $post->id);

        $manager = $this->buildManager();
        $models = $manager->hydrator()->getModels([$post->id], InstrumentedPost::class);

        $native = InstrumentedPost::find($post->id);

        $this->assertSame($native->getConnectionName(), $models[0]->getConnectionName());
        $this->assertSame('testing', $models[0]->getConnectionName());
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

    public function test_cold_miss_with_joined_missed_query_uses_closure_hydration_and_avoids_ambiguous_columns(): void
    {
        $author = Author::create(['name' => 'Owen']);
        $post = InstrumentedPost::create(['title' => 'JoinAmbiguous', 'author_id' => $author->id]);
        $this->evictModelCache(InstrumentedPost::class, $post->id);
        InstrumentedPost::$setRawAttributesCalls = 0;

        $manager = $this->buildManager();
        $joinedQuery = InstrumentedPost::query()->withoutCache()
            ->join('authors', 'authors.id', '=', 'posts.author_id');

        $models = $manager->hydrator()->getModels([$post->id], InstrumentedPost::class, null, null, $joinedQuery, true);

        $this->assertCount(1, $models);
        $this->assertSame('JoinAmbiguous', $models[0]->title);
        $this->assertSame($post->id, $models[0]->id, 'Must resolve the post id, not the colliding authors.id from the join');
        $this->assertSame(0, InstrumentedPost::$setRawAttributesCalls, 'A joined missedQuery is safe via the table-qualified select, so closure hydration must still be used');
    }

    public function test_cold_miss_with_grouped_missed_query_returns_one_row_per_requested_id(): void
    {
        $author = Author::create(['name' => 'Petra']);
        $post1 = InstrumentedPost::create(['title' => 'GroupedOne', 'author_id' => $author->id]);
        $post2 = InstrumentedPost::create(['title' => 'GroupedTwo', 'author_id' => $author->id]);
        $this->evictModelCache(InstrumentedPost::class, $post1->id);
        $this->evictModelCache(InstrumentedPost::class, $post2->id);

        $manager = $this->buildManager();
        // Groups by author_id: post1 and post2 share an author, so a naive
        // whereIn(pk, [post1, post2]) on top of this GROUP BY would collapse to one row.
        $groupedQuery = InstrumentedPost::query()->withoutCache()->groupBy('author_id');

        $models = $manager->hydrator()->getModels([$post1->id, $post2->id], InstrumentedPost::class, null, null, $groupedQuery, true);

        $this->assertCount(2, $models, 'Both requested ids must be resolved, not collapsed by the original query\'s GROUP BY');
        $titles = array_map(fn($m) => $m->title, $models);
        $this->assertEqualsCanonicalizing(['GroupedOne', 'GroupedTwo'], $titles);
    }

    /**
     * Laravel applies whereIn()/where() only to the *first* arm of a union — the second arm is
     * left unconstrained. If prepareMissedQuery() preserved a UNION untouched, a cold-miss
     * refetch over a unioned missedQuery would fetch BOTH the requested row and the unrelated
     * row from the union's second arm, queuing the unrelated row's attributes for caching under
     * its own key as a side effect. The final return value gets filtered back down to the
     * requested ids regardless (see the $ordered loop in getModels()), so the SQL actually
     * executed for the refetch — not the returned models — is the only signal that distinguishes
     * correct behavior from this bug.
     */
    public function test_cold_miss_with_unioned_missed_query_does_not_run_the_union_for_the_refetch(): void
    {
        $author = Author::create(['name' => 'Quinn']);
        $wanted = InstrumentedPost::create(['title' => 'Wanted', 'author_id' => $author->id]);
        InstrumentedPost::create(['title' => 'Unrequested', 'author_id' => $author->id]);
        $this->evictModelCache(InstrumentedPost::class, $wanted->id);

        $manager = $this->buildManager();
        $unionedQuery = InstrumentedPost::query()->withoutCache()->where('title', 'Wanted');
        $unionedQuery->getQuery()->union(
            InstrumentedPost::query()->withoutCache()->where('title', 'Unrequested')->getQuery()
        );

        DB::enableQueryLog();
        $models = $manager->hydrator()->getModels([$wanted->id], InstrumentedPost::class, null, null, $unionedQuery, true);
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertCount(1, $models);
        $this->assertSame('Wanted', $models[0]->title);

        $refetchRanAUnion = array_filter($queries, fn($q) => str_contains(strtolower($q['query']), 'union'));
        $this->assertSame([], $refetchRanAUnion, 'The original union must not be reused for the by-id refetch — it leaves the second arm unconstrained by the requested ids');
    }
}
