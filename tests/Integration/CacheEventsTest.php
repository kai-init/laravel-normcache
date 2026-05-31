<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\Event;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class CacheEventsTest extends TestCase
{
    public function test_query_cache_miss_fired_on_first_get(): void
    {
        Event::fake([QueryCacheMiss::class]);

        Author::create(['name' => 'Alice']);
        Author::all();

        Event::assertDispatched(QueryCacheMiss::class, function (QueryCacheMiss $e) {
            return $e->modelClass === Author::class;
        });
    }

    public function test_query_cache_hit_fired_on_subsequent_get(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        Event::fake([QueryCacheHit::class]);

        Author::all();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return $e->modelClass === Author::class;
        });
    }

    public function test_query_cache_miss_not_fired_on_hit(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        Event::fake([QueryCacheMiss::class]);

        Author::all();

        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_query_cache_hit_not_fired_on_miss(): void
    {
        Event::fake([QueryCacheHit::class]);

        Author::create(['name' => 'Alice']);
        Author::all();

        Event::assertNotDispatched(QueryCacheHit::class);
    }

    public function test_model_cache_miss_fired_when_models_not_in_cache(): void
    {
        Event::fake([ModelCacheMiss::class]);

        $author = Author::create(['name' => 'Alice']);
        Author::all();

        Event::assertDispatched(ModelCacheMiss::class, function (ModelCacheMiss $e) use ($author) {
            return $e->modelClass === Author::class
                && in_array($author->id, $e->ids);
        });
    }

    public function test_model_cache_hit_fired_when_models_in_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        Event::fake([ModelCacheHit::class]);

        Author::all();

        Event::assertDispatched(ModelCacheHit::class, function (ModelCacheHit $e) use ($author) {
            return $e->modelClass === Author::class
                && in_array($author->id, $e->ids);
        });
    }

    public function test_partial_model_cache_miss_fires_both_events(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Author::all(); // warm alice's model key

        $bob = Author::create(['name' => 'Bob']); // bob's model key not cached yet

        Event::fake([ModelCacheHit::class, ModelCacheMiss::class]);

        // query cache is stale (version bumped by Bob's insert), so both IDs are fetched via MGET
        Author::all();

        Event::assertDispatched(ModelCacheHit::class);
        Event::assertDispatched(ModelCacheMiss::class);
    }

    public function test_query_cache_hit_event_carries_correct_key(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        Event::fake([QueryCacheHit::class]);

        Author::all();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return str_starts_with($e->key, 'query:{' . app('normcache')->classKey(Author::class) . '}:v');
        });
    }

    public function test_no_events_fired_when_cache_bypassed_with_without_cache(): void
    {
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        Author::create(['name' => 'Alice']);
        Author::withoutCache()->get();

        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_raw_depends_on_miss_fires_query_cache_miss(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Event::fake([QueryCacheMiss::class]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        Event::assertDispatched(QueryCacheMiss::class, function (QueryCacheMiss $e) {
            return $e->modelClass === Author::class
                && str_starts_with($e->key, 'raw:{' . app('normcache')->classKey(Author::class) . '}:');
        });
    }

    public function test_raw_depends_on_miss_does_not_fire_model_cache_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Event::fake([ModelCacheHit::class]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        Event::assertNotDispatched(ModelCacheHit::class);
    }

    public function test_through_relation_cache_fires_query_events(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Event::fake([QueryCacheMiss::class]);

        $country->posts()->get();

        Event::assertDispatched(QueryCacheMiss::class, function (QueryCacheMiss $e) {
            return $e->modelClass === Post::class
                && str_starts_with($e->key, 'through:{' . app('normcache')->classKey(Post::class) . '}:');
        });

        Event::fake([QueryCacheHit::class]);

        $country->posts()->get();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return $e->modelClass === Post::class
                && str_starts_with($e->key, 'through:{' . app('normcache')->classKey(Post::class) . '}:');
        });
    }

    public function test_relation_aggregate_cache_fires_query_events(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Event::fake([QueryCacheMiss::class]);

        Author::withCount('posts')->get();

        Event::assertDispatched(QueryCacheMiss::class, function (QueryCacheMiss $e) {
            return $e->modelClass === Author::class
                && str_starts_with($e->key, 'agg:{' . app('normcache')->classKey(Author::class) . '}:');
        });

        Event::fake([QueryCacheHit::class]);

        Author::withCount('posts')->get();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return $e->modelClass === Author::class
                && str_starts_with($e->key, 'agg:{' . app('normcache')->classKey(Author::class) . '}:');
        });
    }
}
