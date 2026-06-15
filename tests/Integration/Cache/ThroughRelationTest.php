<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: hasManyThrough results are cached and invalidated correctly;
 * join columns must not contaminate the model cache, and queries with extra joins
 * delegate to Eloquent.
 */
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

        Redis::connection('normcache-test')
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

        Redis::connection('normcache-test')
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

    public function test_through_relation_raw_where_bindings_affect_cache_identity(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'A', 'author_id' => $author->id]);
        Post::create(['title' => 'B', 'author_id' => $author->id]);

        $first = $country->posts()
            ->whereRaw('posts.title = ?', ['A'])
            ->get();

        $second = $country->posts()
            ->whereRaw('posts.title = ?', ['B'])
            ->get();

        $this->assertSame(['A'], $first->pluck('title')->all());
        $this->assertSame(['B'], $second->pluck('title')->all());
    }

    public function test_through_relation_subquery_where_does_not_go_stale(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        DB::table('comments')->insert([
            'body' => 'c1',
            'commentable_type' => Post::class,
            'commentable_id' => $post->id,
        ]);

        $warm = $country->posts()
            ->whereExists(function ($q) {
                $q->from('comments')->whereColumn('comments.commentable_id', 'posts.id');
            })
            ->get();
        $this->assertSame(['Hello'], $warm->pluck('title')->all());

        // Remove the only comment: the whereExists no longer matches the post.
        DB::table('comments')->where('commentable_id', $post->id)->delete();

        $after = $country->posts()
            ->whereExists(function ($q) {
                $q->from('comments')->whereColumn('comments.commentable_id', 'posts.id');
            })
            ->get();

        $this->assertSame([], $after->pluck('title')->all());
    }

    public function test_through_wildcard_plus_extra_column_does_not_pollute_model_cache(): void
    {
        $country = Country::create(['name' => 'UK']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $country->posts()->select('posts.*')->selectRaw('2 as polluted')->get();

        $cached = NormCache::getModels([$post->id], Post::class);
        $this->assertArrayNotHasKey('polluted', $cached[0]->getRawOriginal());
    }
}
