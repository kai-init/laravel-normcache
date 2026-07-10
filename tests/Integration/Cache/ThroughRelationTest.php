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

        $this->assertNotNull($cached, 'Through loads should populate the per-id model cache');
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

        $keyCountAfterFirst = count($this->redisKeys('*'));

        $second = $country->posts()->get()->pluck('title');

        $this->assertEquals($first, $second);
        $this->assertSame($keyCountAfterFirst, count($this->redisKeys('*')));
    }

    public function test_simple_has_many_through_warm_hit_refetches_only_evicted_child_model(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $first = $country->posts()->get();

        $this->assertSame(['Hello'], $first->pluck('title')->all());
        $this->assertNotEmpty($this->redisKeys('through:*'));

        Redis::connection('normcache-test')
            ->del($this->prefixedModelKey(Post::class, $post->id));

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $second = $country->posts()->get();

        $this->assertSame(['Hello'], $second->pluck('title')->all());
        $this->assertSame(
            1,
            $queryCount,
            'Expected normalized through cache to refetch only the evicted Post row, not the whole relation'
        );
    }

    public function test_through_relation_corrupt_query_payload_degrades_to_miss_and_repairs(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->get();

        $queryKey = collect($this->redisKeys('through:*'))->first();
        $this->assertNotNull($queryKey);

        Redis::connection('normcache-test')->set($queryKey, 'NOT_JSON');

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = $country->posts()->get();

        $this->assertGreaterThan(0, $queryCount, 'Corrupt through payload should fall through to a fresh DB query');
        $this->assertSame([$post->id], $results->pluck('id')->all());

        $raw = Redis::connection('normcache-test')->get($queryKey);
        $repaired = json_decode($raw, true);
        $this->assertIsArray($repaired);
        $this->assertSame([(string) $post->id], $repaired['i']);
    }

    public function test_flush_tag_removes_tagged_through_relation_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->tag('homepage')->get();

        $this->assertNotEmpty($this->redisKeys('through:*:homepage:*'));

        NormCache::flushTag(Post::class, 'homepage');

        $this->assertEmpty($this->redisKeys('through:*:homepage:*'));
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

        $canonicalColumns = array_keys((array) DB::table('posts')->find($post->id));

        $this->assertNotNull($cached, 'The evicted Post should be refetched and recached');
        $this->assertArrayNotHasKey('laravel_through_key', $cached);
        $this->assertEmpty(
            array_diff(array_keys($cached), $canonicalColumns),
            'No join columns should leak into the cached attributes'
        );
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
        $this->assertEmpty($this->redisKeys('through:*'));
    }

    public function test_outdated_through_cache_entry_can_remain_after_through_model_version_bump(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $country->posts()->get();

        $oldPostVersion = NormCache::currentVersion(Post::class);
        $oldAuthorVersion = NormCache::currentVersion(Author::class);

        $author->update(['name' => 'Alice Updated']);

        $this->assertGreaterThan($oldAuthorVersion, NormCache::currentVersion(Author::class));

        $orphanedKeys = $this->redisKeys("through:*:v{$oldPostVersion}:v{$oldAuthorVersion}:*");

        $this->assertNotEmpty($orphanedKeys);
        $this->assertSame(['Hello'], $country->posts()->get()->pluck('title')->all());
    }

    public function test_without_cache_bypasses_has_many_through_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);

        $country->posts()->get();

        // The version bump here makes the through cache entry outdated, but withoutCache() must skip it regardless.
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

    public function test_through_relation_subquery_where_does_not_serve_outdated_data(): void
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

        $cached = NormCache::hydrator()->getModels([$post->id], Post::class);
        $this->assertArrayNotHasKey('polluted', $cached[0]->getRawOriginal());
    }

    public function test_lazy_has_many_through_cache_is_parent_specific(): void
    {
        $au = Country::create(['name' => 'Australia']);
        $ca = Country::create(['name' => 'Canada']);

        $auAuthor = Author::create(['name' => 'Alice', 'country_id' => $au->id]);
        $caAuthor = Author::create(['name' => 'Bob', 'country_id' => $ca->id]);

        Post::create(['title' => 'AU Post', 'author_id' => $auAuthor->id]);
        Post::create(['title' => 'CA Post', 'author_id' => $caAuthor->id]);

        $this->assertSame(['AU Post'], $au->posts()->get()->pluck('title')->all());
        $this->assertSame(['CA Post'], $ca->posts()->get()->pluck('title')->all());
    }

    public function test_eager_has_many_through_cache_is_parent_set_specific(): void
    {
        $au = Country::create(['name' => 'Australia']);
        $ca = Country::create(['name' => 'Canada']);

        $auAuthor = Author::create(['name' => 'Alice', 'country_id' => $au->id]);
        $caAuthor = Author::create(['name' => 'Bob', 'country_id' => $ca->id]);

        Post::create(['title' => 'AU Post', 'author_id' => $auAuthor->id]);
        Post::create(['title' => 'CA Post', 'author_id' => $caAuthor->id]);

        $countries = Country::with('posts')->get()->keyBy('name');

        $this->assertSame(['AU Post'], $countries['Australia']->posts->pluck('title')->all());
        $this->assertSame(['CA Post'], $countries['Canada']->posts->pluck('title')->all());
    }
}
