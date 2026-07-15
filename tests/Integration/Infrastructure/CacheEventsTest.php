<?php

namespace NormCache\Tests\Integration\Infrastructure;

use Illuminate\Support\Facades\Event;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: QueryCacheMiss, QueryCacheHit, ModelCacheMiss, ModelCacheHit, and
 * QueryBypassed events are fired with correct payloads on every cache path.
 */
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

    public function test_partial_model_cache_miss_fires_miss_for_both_when_version_bumped(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Author::all(); // warm alice's model key at version V

        Author::create(['name' => 'Bob']); // version bumps to V+1; alice's V key is no longer current

        Event::fake([ModelCacheHit::class, ModelCacheMiss::class]);

        // query cache is stale; version bump makes alice's model key unreachable too, so both miss
        Author::all();

        Event::assertNotDispatched(ModelCacheHit::class);
        Event::assertDispatched(ModelCacheMiss::class);
    }

    public function test_query_cache_hit_event_carries_correct_key(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        Event::fake([QueryCacheHit::class]);

        Author::all();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return str_starts_with($e->key, app('normcache')->keys()->prefixed('query:' . app('normcache')->keys()->classKey(Author::class) . ':v'));
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

    public function test_result_depends_on_miss_fires_query_cache_miss(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Event::fake([QueryCacheMiss::class]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        Event::assertDispatched(QueryCacheMiss::class, function (QueryCacheMiss $e) {
            return $e->modelClass === Author::class
                && str_starts_with($e->key, app('normcache')->keys()->prefixed('result:' . app('normcache')->keys()->classKey(Author::class) . ':'));
        });
    }

    public function test_result_depends_on_miss_does_not_fire_model_cache_hit(): void
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
                && str_starts_with($e->key, app('normcache')->keys()->prefixed('through:' . app('normcache')->keys()->classKey(Post::class) . ':'));
        });

        Event::fake([QueryCacheHit::class]);

        $country->posts()->get();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return $e->modelClass === Post::class
                && str_starts_with($e->key, app('normcache')->keys()->prefixed('through:' . app('normcache')->keys()->classKey(Post::class) . ':'));
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
                && str_starts_with($e->key, app('normcache')->keys()->prefixed('result:' . app('normcache')->keys()->classKey(Author::class) . ':'));
        });

        Event::fake([QueryCacheHit::class]);

        Author::withCount('posts')->get();

        Event::assertDispatched(QueryCacheHit::class, function (QueryCacheHit $e) {
            return $e->modelClass === Author::class
                && str_starts_with($e->key, app('normcache')->keys()->prefixed('result:' . app('normcache')->keys()->classKey(Author::class) . ':'));
        });
    }
}
