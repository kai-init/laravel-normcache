<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class ThroughRelationTest extends TestCase
{
    public function test_through_load_does_not_store_join_artifact_in_model_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->get();

        $cached = $this->modelCacheEntry(Post::class, $post->id);

        $this->assertIsArray($cached);
        $this->assertArrayNotHasKey('laravel_through_key', $cached);
    }

    public function test_post_found_after_through_load_has_no_spurious_through_key(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->get();

        $fetched = Post::find($post->id);

        $this->assertArrayNotHasKey('laravel_through_key', $fetched->getRawOriginal());
        $this->assertArrayNotHasKey('laravel_through_key', $fetched->toArray());
    }

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

        $country->posts()->get();

        $post->update(['title' => 'Updated']);

        $title = $country->posts()->get()->first()->title;

        $this->assertSame('Updated', $title);
    }

    public function test_through_relation_join_columns_do_not_contaminate_model_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        Author::create(['name' => 'Dummy']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->get();

        Redis::connection('model-cache-test')
            ->del($this->prefixedModelKey(Post::class, $post->id));

        $country->posts()->get();

        $cached = $this->modelCacheEntry(Post::class, $post->id);

        $this->assertIsArray($cached);
        $this->assertArrayNotHasKey('name', $cached);
        $this->assertArrayNotHasKey('country_id', $cached);
    }

    public function test_through_relation_model_cache_miss_does_not_corrupt_post_id(): void
    {
        $country = Country::create(['name' => 'Australia']);
        Author::create(['name' => 'Dummy']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $this->assertNotSame($post->id, $author->id);

        $country->posts()->get();

        Redis::connection('model-cache-test')
            ->del($this->prefixedModelKey(Post::class, $post->id));

        $results = $country->posts()->get();

        $this->assertCount(1, $results);
        $this->assertSame($post->id, $results->first()->getKey());
    }

    public function test_through_relation_with_extra_join_delegates_to_eloquent(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $posts = $country->posts()
            ->join('countries as c2', 'c2.id', '=', 'authors.country_id')
            ->get();

        $this->assertSame([$post->id], $posts->modelKeys());
        $this->assertEmpty($this->redisKeys('test:through:*'));
    }

    public function test_stale_through_cache_entry_can_remain_after_through_model_version_bump(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->get();

        $stalePostVersion = NormCache::currentVersion(Post::class);
        $staleAuthorVersion = NormCache::currentVersion(Author::class);

        $author->update(['name' => 'Alice Updated']);

        $this->assertGreaterThan($staleAuthorVersion, NormCache::currentVersion(Author::class));

        $staleKeys = $this->redisKeys("test:through:*:v{$stalePostVersion}:v{$staleAuthorVersion}:*");

        $this->assertNotEmpty($staleKeys);
        $this->assertSame(['Hello'], $country->posts()->get()->pluck('title')->all());
    }

    public function test_without_cache_bypasses_has_many_through_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);

        $country->posts()->get();

        // The version bump here makes the through cache stale, but withoutCache() must skip it regardless.
        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        DB::enableQueryLog();
        $country->posts()->withoutCache()->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotEmpty($queries, 'withoutCache() on HasManyThrough should issue a DB query');
    }
}
