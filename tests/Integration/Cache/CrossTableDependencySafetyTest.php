<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: queries touching multiple tables must bypass caching when
 * cross-table dependencies are not declared, and cache correctly when dependsOn is used.
 */
class CrossTableDependencySafetyTest extends TestCase
{
    public function test_join_count_without_depends_on_infers_join_dependency_and_caches(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->count();

        $this->assertNotEmpty($this->redisKeys('count:*'));
    }

    public function test_join_count_with_depends_on_caches(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->count();

        $this->assertNotEmpty($this->redisKeys('count:*'));
    }

    public function test_join_count_with_depends_on_invalidates_on_dep_change(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $query = fn() => Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->count();

        $this->assertSame(1, $query());
        $this->assertSame(1, $query());

        Post::create(['title' => 'World', 'author_id' => $author->id]);

        $this->assertSame(2, $query());
    }

    public function test_join_paginate_without_depends_on_infers_join_dependency_and_caches_count(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->paginate(10);

        $this->assertNotEmpty($this->redisKeys('count:*'));
    }

    public function test_join_paginate_with_depends_on_caches_count(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->paginate(10);

        $this->assertNotEmpty($this->redisKeys('count:*'));
    }

    public function test_simple_count_without_join_still_caches(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()->count();

        $this->assertNotEmpty($this->redisKeys('count:*'));
    }

    public function test_count_with_group_by_single_table_still_caches(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()->groupBy('name')->count('name');

        $this->assertNotEmpty($this->redisKeys('count:*'));
    }

    public function test_aggregate_constraint_with_join_disables_tracking_and_bypasses(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::withCount([
            'posts' => fn($q) => $q->join('authors', 'authors.id', '=', 'posts.author_id'),
        ])->get();

        $this->assertEmpty($this->redisKeys('result:*'));
    }

    public function test_simple_aggregate_constraint_still_infers_dependencies(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::withCount(['posts' => fn($q) => $q->where('title', 'Hello')])->get();

        $this->assertNotEmpty($this->redisKeys('result:*'));
    }

    public function test_aggregate_constraint_with_wherehas_disables_tracking(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::withCount([
            'posts' => fn($q) => $q->whereHas('author'),
        ])->get();

        $this->assertEmpty($this->redisKeys('result:*'));
    }

    public function test_aliased_from_query_bypasses_normalized_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::from('authors as a')->where('a.name', 'Alice')->get();

        $this->assertEmpty($this->redisKeys('query:*'));
    }

    public function test_aliased_from_with_depends_on_uses_result_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::from('authors as a')
            ->where('a.name', 'Alice')
            ->dependsOn([Author::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('result:*'));
    }

    public function test_canonical_from_still_uses_normalized_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::from('authors')->get();

        $this->assertNotEmpty($this->redisKeys('query:*'));
    }

    public function test_pluck_with_falsey_key_hashes_differently_from_no_key(): void
    {
        Author::create(['name' => 'Alice']);

        // Warm cache for pluck without a key
        Author::query()->pluck('name');
        $noKeyCache = $this->redisKeys('scalar:*');

        // Pluck with an integer key (falsey: 0)
        Author::query()->pluck('name', 'id');
        $withKeyCache = $this->redisKeys('scalar:*');

        // Should have two separate cache entries, not share one
        $this->assertCount(2, $withKeyCache);
        $this->assertNotEmpty(array_diff($withKeyCache, $noKeyCache));
    }

    public function test_pluck_with_null_key_behaves_same_as_no_key(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()->pluck('name', null);
        $nullKeyCache = $this->redisKeys('scalar:*');

        Author::query()->pluck('name');
        $noKeyCache = $this->redisKeys('scalar:*');

        // null key should hash the same as no key (both use only [$column])
        $this->assertCount(1, $noKeyCache);
        $this->assertSame(count($nullKeyCache), count($noKeyCache));
    }
}
