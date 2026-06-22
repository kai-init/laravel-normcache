<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: JOIN queries must bypass the cache without an explicit column
 * selection; with dependsOn() and an explicit root-table select they are stored and
 * served as result-cache entries.
 */
class JoinResultCacheTest extends TestCase
{
    public function test_join_with_depends_on_and_no_explicit_select_bypasses_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_join_with_depends_on_and_no_explicit_select_returns_correct_results(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $results = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->get();

        $this->assertCount(1, $results);
    }

    public function test_join_with_depends_on_and_explicit_root_select_caches_as_result(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'));
    }

    public function test_join_with_explicit_select_serves_subsequent_calls_from_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->get();

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $results = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->get();

        $this->assertSame(0, $queryCount);
        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }

    public function test_plain_join_without_depends_on_infers_table_dependency_and_caches(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'));
    }

    public function test_plain_join_without_explicit_root_select_bypasses_despite_inferred_tables(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'));
        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_inferred_join_invalidates_when_joined_table_is_written(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $first = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->get();
        $this->assertCount(1, $first);

        $author2 = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'World', 'author_id' => $author2->id]);

        $second = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->get();
        $this->assertCount(2, $second);
    }

    public function test_join_get_star_string_without_explicit_root_select_bypasses_result_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $results = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->get('*');

        $this->assertCount(1, $results);
        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_expression_join_bypasses_inferred_dependency(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join(DB::raw('posts p'), 'p.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_join_with_where_exists_clause_bypasses_auto_inference(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        DB::table('comments')->insert([
            'body' => 'c1',
            'commentable_type' => Post::class,
            'commentable_id' => $post->id,
        ]);

        Author::query()
            ->join('posts', function ($join) {
                $join->on('posts.author_id', '=', 'authors.id')
                    ->whereExists(function ($query) {
                        $query
                            ->from('comments')
                            ->whereColumn('comments.commentable_id', 'posts.id')
                            ->where('comments.commentable_type', Post::class);
                    });
            })
            ->select('authors.*')
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_join_with_raw_clause_bypasses_auto_inference(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', function ($join) {
                $join->on('posts.author_id', '=', 'authors.id')
                    ->whereRaw('posts.title <> ?', ['Draft']);
            })
            ->select('authors.*')
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_implicit_join_alias_bypasses_auto_inference(): void
    {
        $builder = Author::query()
            ->join('posts p', 'p.author_id', '=', 'authors.id')
            ->select('authors.*');
        $prepared = $builder->prepareCacheExecution();

        $dependencies = (new QueryAnalyzer)->inferJoinDependencies(
            $prepared->base,
            $builder->getModel()->getConnection()->getName()
        );

        $this->assertFalse($dependencies->safe);
    }

    public function test_multiple_joins_all_table_deps_collected_and_invalidated(): void
    {
        $country = Country::create(['name' => 'AU']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $first = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->join('countries', 'countries.id', '=', 'authors.country_id')
            ->where('countries.name', 'AU')
            ->select('authors.*')
            ->get();
        $this->assertCount(1, $first);

        $this->assertNotEmpty($this->redisKeys('test:result:*'));

        $country->update(['name' => 'NZ']);

        $second = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->join('countries', 'countries.id', '=', 'authors.country_id')
            ->where('countries.name', 'AU')
            ->select('authors.*')
            ->get();

        $this->assertCount(0, $second);
    }

    public function test_plain_join_count_infers_join_dependency_and_invalidates(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $count1 = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->count();

        $this->assertSame(1, $count1);

        Post::create(['title' => 'P2', 'author_id' => $author->id]);

        $count2 = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->count();

        $this->assertSame(2, $count2);
    }

    public function test_plain_join_paginate_infers_join_dependency_and_invalidates(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $page1 = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->paginate(10);

        $this->assertSame(1, $page1->total());

        Post::create(['title' => 'P2', 'author_id' => $author->id]);

        $page2 = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->paginate(10);

        $this->assertSame(2, $page2->total());
    }

    public function test_join_to_non_cacheable_table_infers_table_dep_and_caches(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        // author_tag has no Cacheable model — dep is tracked as a table version key
        Author::query()
            ->join('author_tag', 'author_tag.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'));
    }

    public function test_corrupt_result_cache_payload_is_treated_as_miss(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $query = Author::query()->whereHas('posts')->dependsOn([Post::class]);
        $query->get(); // warm

        $resultKey = collect($this->redisKeys('test:result:*'))->first();
        Redis::connection('normcache-test')->set($resultKey, 'CORRUPT');

        $results = $query->get();

        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }
}
