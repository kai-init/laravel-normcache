<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class ThroughRelationTest extends TestCase
{
    public function test_has_many_through_caches_results(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $first = $country->posts()->get()->pluck('title');

        $keyCountAfterFirst = count($this->redisKeys('test:*'));

        $second = $country->posts()->get()->pluck('title');

        $this->assertEquals($first, $second);
        $this->assertSame($keyCountAfterFirst, count($this->redisKeys('test:*')));
    }

    public function test_has_many_through_returns_correct_results(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);
        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        $titles = $country->posts()->get()->pluck('title')->sort()->values();

        $this->assertSame(['Post A', 'Post B'], $titles->all());
    }

    public function test_has_many_through_cache_invalidated_when_post_version_changes(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);

        $country->posts()->get();

        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        $titles = $country->posts()->get()->pluck('title')->sort()->values();

        $this->assertSame(['Post A', 'Post B'], $titles->all());
    }

    public function test_updating_related_model_reflected_after_through_cache_warm(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Original', 'author_id' => $author->id]);

        $country->posts()->get(); // warm through + model cache

        $post->update(['title' => 'Updated']); // flushes model key, bumps version

        $title = $country->posts()->get()->first()->title;

        $this->assertSame('Updated', $title);
    }

    public function test_without_cache_bypasses_has_many_through_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);

        // Warm the through cache.
        $country->posts()->get();

        // Add a new post — version bump makes through cache stale, but we want
        // to verify withoutCache() goes to DB regardless of cache state.
        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        DB::enableQueryLog();
        $country->posts()->withoutCache()->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotEmpty($queries, 'withoutCache() on HasManyThrough should issue a DB query');
    }

    public function test_without_cache_on_through_relation_returns_fresh_data(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);

        $country->posts()->get(); // warm cache

        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        $titles = $country->posts()->withoutCache()->get()->pluck('title')->sort()->values();

        $this->assertSame(['Post A', 'Post B'], $titles->all());
    }
}
