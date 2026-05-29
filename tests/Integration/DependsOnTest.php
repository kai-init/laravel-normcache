<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class DependsOnTest extends TestCase
{
    public function test_depends_on_caches_where_has_query(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id, 'published' => true]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:raw:*'));
    }

    public function test_depends_on_returns_correct_results(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Hello', 'author_id' => $alice->id]);

        $results = Author::whereHas('posts')->dependsOn([Post::class])->get();

        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }

    public function test_depends_on_cache_hits_on_second_call(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        $this->assertSame(0, $queryCount);
    }

    public function test_depends_on_invalidates_on_primary_model_version_bump(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $first = Author::whereHas('posts')->dependsOn([Post::class])->get();
        $this->assertCount(1, $first);

        Author::create(['name' => 'Bob']);

        $second = Author::whereHas('posts')->dependsOn([Post::class])->get();
        $this->assertCount(1, $second);
    }

    public function test_depends_on_invalidates_on_dep_model_version_bump(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id, 'published' => true]);

        $first = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->get();
        $this->assertCount(1, $first);

        $post->update(['published' => false]);

        $second = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->get();
        $this->assertCount(0, $second);
    }

    public function test_depends_on_multiple_deps_invalidates_on_any_dep_bump(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Hello', 'author_id' => $alice->id]);

        $first = Author::whereHas('posts')->dependsOn([Post::class, Author::class])->get();
        $this->assertCount(1, $first);

        // Bump Post version — Bob now also has a post
        Post::create(['title' => 'Bob Post', 'author_id' => $bob->id]);

        $second = Author::whereHas('posts')->dependsOn([Post::class, Author::class])->get();
        $this->assertCount(2, $second);
    }

    public function test_depends_on_dep_order_does_not_affect_key(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class, Author::class])->get();
        $keysAB = $this->redisKeys('test:query:*');

        // Running with reversed dep order should hit the same cache key — no new key written
        Author::whereHas('posts')->dependsOn([Author::class, Post::class])->get();
        $keysBA = $this->redisKeys('test:query:*');

        $this->assertSame(
            array_map(fn($k) => str_replace('test:', '', $k), $keysAB),
            array_map(fn($k) => str_replace('test:', '', $k), $keysBA)
        );
    }

    public function test_depends_on_paginate_caches_count_with_dep_versions(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->paginate(10);

        $this->assertNotEmpty($this->redisKeys('test:count:*'));

        // creating a post bumps Post's version, making the count key stale
        Post::create(['title' => 'World', 'author_id' => $author->id]);

        $firstKeys = $this->redisKeys('test:count:*');

        Author::whereHas('posts')->dependsOn([Post::class])->paginate(10);

        $secondKeys = $this->redisKeys('test:count:*');

        // two distinct versioned count keys now exist in Redis: the old stale one and a new one
        $this->assertCount(2, $secondKeys);
        $this->assertNotEmpty(array_diff($secondKeys, $firstKeys));
    }

    public function test_depends_on_count_invalidates_on_dep_model_version_bump(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id, 'published' => true]);

        $first = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->count();
        $this->assertSame(1, $first);

        Post::query()->update(['published' => false]);

        $second = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->count();
        $this->assertSame(0, $second);
    }

    public function test_depends_on_count_hits_cache_on_second_call(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->count();

        DB::enableQueryLog();
        Author::whereHas('posts')->dependsOn([Post::class])->count();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
    }

    public function test_explain_returns_cached_when_depends_on_set(): void
    {
        $result = Author::whereHas('posts')->dependsOn([Post::class])->explain();

        $this->assertStringContainsString('cached', $result);
        $this->assertStringContainsString('dependsOn()', $result);
    }

    public function test_join_with_depends_on_caches_as_blob(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:raw:*'));
    }

    public function test_explain_shows_computed_blob_for_join_with_depends_on(): void
    {
        $result = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->explain();

        $this->assertStringContainsString('cached', $result);
        $this->assertStringContainsString('raw (dependsOn())', $result);
    }

    public function test_from_subquery_with_depends_on_caches_as_blob(): void
    {
        Author::create(['name' => 'Alice']);

        Author::fromSub(Author::query()->select('id', 'name'), 'authors')
            ->dependsOn([Author::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:raw:*'));
    }

    public function test_raw_order_with_depends_on_can_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()
            ->orderByRaw('CASE WHEN name = ? THEN 0 ELSE 1 END', ['Alice'])
            ->dependsOn([Author::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:raw:*'));
    }

    public function test_where_in_subquery_requires_depends_on(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->whereIn('id', Post::query()->select('author_id'))
            ->get();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_where_in_subquery_with_depends_on_can_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->whereIn('id', Post::query()->select('author_id'))
            ->dependsOn([Post::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:raw:*'));
    }

    public function test_behaviora_l_distinct_with_depends_on_preserves_distinct_semantics(): void
    {
        $a = Author::create(['name' => 'A']);
        $b = Author::create(['name' => 'B']);
        Post::create(['title' => 'p1', 'author_id' => $a->id]);
        Post::create(['title' => 'p2', 'author_id' => $a->id]);
        Post::create(['title' => 'p3', 'author_id' => $b->id]);

        $uncached = Post::query()->select('author_id')->distinct()->withoutCache()->get();
        $cached = Post::query()->select('author_id')->distinct()->dependsOn([Post::class])->get();

        $this->assertSame(
            count($uncached),
            count($cached),
            'DISTINCT queries with dependsOn() use the blob path — both return the same deduplicated row count.'
        );
    }

    public function test_behaviora_l_lock_for_update_with_depends_on_hits_the_db(): void
    {
        $a = Author::create(['name' => 'A']);
        Post::create(['title' => 'p1', 'author_id' => $a->id, 'published' => true]);

        Post::query()->where('published', true)->dependsOn([Post::class])->get();

        DB::enableQueryLog();
        DB::flushQueryLog();

        Post::query()->where('published', true)->lockForUpdate()->dependsOn([Post::class])->get();

        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotEmpty($queries, 'lockForUpdate queries hit the DB even when dependsOn() is set.');
    }

    public function test_behaviora_l_aggregate_columns_fall_through_to_db_with_depends_on(): void
    {
        $this->seedAuthorsWithPosts();

        $uncached = Post::query()
            ->select('author_id', DB::raw('SUM(views) as sum_views'))
            ->groupBy('author_id')
            ->withoutCache()
            ->get();

        $cached = Post::query()
            ->select('author_id', DB::raw('SUM(views) as sum_views'))
            ->groupBy('author_id')
            ->dependsOn([Post::class])
            ->get();

        $this->assertNotNull($cached->first(), 'Query returns results.');
        $this->assertNotNull(
            $cached->first()->getAttribute('sum_views'),
            'sum_views is populated — GROUP BY queries use the blob path with dependsOn().'
        );
    }

    // -------------------------------------------------------------------------
    // tag() — manual flush grouping
    // -------------------------------------------------------------------------

    public function test_tag_is_embedded_in_computed_key(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

        $this->assertNotEmpty($this->redisKeys('test:raw:*:homepage:*'));
        $this->assertEmpty($this->redisKeys('test:raw:*[^:]homepage*'));
    }

    public function test_tagged_keys_are_isolated_from_untagged_keys(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();
        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

        $all = $this->redisKeys('test:raw:*');
        $tagged = $this->redisKeys('test:raw:*:homepage:*');

        $this->assertCount(2, $all);
        $this->assertCount(1, $tagged);
    }

    public function test_flush_tag_removes_only_matching_keys(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();
        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

        $removed = \NormCache\Facades\NormCache::flushTag(Author::class, 'homepage');

        $this->assertSame(1, $removed);
        $this->assertNotEmpty($this->redisKeys('test:raw:*'));
        $this->assertEmpty($this->redisKeys('test:raw:*:homepage:*'));
    }

    public function test_flush_tag_across_models_removes_all_matching(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->tag('deploy')->get();
        Post::query()->dependsOn([Author::class])->tag('deploy')->get();

        $removed = \NormCache\Facades\NormCache::flushTagAcrossModels('deploy');

        $this->assertSame(2, $removed);
        $this->assertEmpty($this->redisKeys('test:raw:*:deploy:*'));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private function seedAuthorsWithPosts(): void
    {
        $a = Author::create(['name' => 'A']);
        $b = Author::create(['name' => 'B']);
        Post::create(['title' => 'p1', 'author_id' => $a->id, 'views' => 10, 'published' => true]);
        Post::create(['title' => 'p2', 'author_id' => $a->id, 'views' => 20, 'published' => false]);
        Post::create(['title' => 'p3', 'author_id' => $b->id, 'views' => 30, 'published' => true]);
    }
}
