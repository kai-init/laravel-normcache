<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: dependsOn() API — verifies that explicit cross-class dependencies
 * are cached, invalidated on version bumps, and handled correctly for paginate and count.
 */
class DependsOnTest extends TestCase
{
    public function test_simple_depends_on_query_uses_normalized_query_cache(): void
    {
        if ($this->cacheManager()->isSlotting()) {
            $this->markTestSkipped('In slotting mode, multi-dependency queries route to result cache — see ClusterModeTest');
        }

        Author::create(['name' => 'Alice']);

        Author::query()->dependsOn([Post::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:query:*'));
        $this->assertEmpty($this->redisKeys('test:result:*'));
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

        // dep order is sorted before hashing, so reversed order must hit the same key
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

        // inserting bumps Post's version; the old count key becomes unreachable but is not deleted
        Post::create(['title' => 'World', 'author_id' => $author->id]);

        $firstKeys = $this->redisKeys('test:count:*');

        Author::whereHas('posts')->dependsOn([Post::class])->paginate(10);

        $secondKeys = $this->redisKeys('test:count:*');

        // two distinct versioned count keys: the stale orphan and the new one
        $this->assertCount(2, $secondKeys);
        $this->assertNotEmpty(array_diff($secondKeys, $firstKeys));
    }

    public function test_join_with_depends_on_paginate_caches_count(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Hello', 'author_id' => $alice->id]);
        Post::create(['title' => 'World', 'author_id' => $bob->id]);

        $pageOne = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->orderBy('authors.id')
            ->dependsOn([Post::class])
            ->paginate(1, ['authors.*'], 'page', 1);

        $this->assertSame(2, $pageOne->total());
        $this->assertCount(1, $pageOne->items());
        $this->assertSame('Alice', $pageOne->first()->name);
        $this->assertNotEmpty($this->redisKeys('test:count:*'));

        $pageTwo = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->orderBy('authors.id')
            ->dependsOn([Post::class])
            ->paginate(1, ['authors.*'], 'page', 2);

        $this->assertSame(2, $pageTwo->total());
        $this->assertCount(1, $pageTwo->items());
        $this->assertSame('Bob', $pageTwo->first()->name);

        DB::enableQueryLog();
        DB::flushQueryLog();

        $cachedPageTwo = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->orderBy('authors.id')
            ->dependsOn([Post::class])
            ->paginate(1, ['authors.*'], 'page', 2);

        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame(2, $cachedPageTwo->total());
        $this->assertCount(1, $cachedPageTwo->items());
        $this->assertSame('Bob', $cachedPageTwo->first()->name);
        $this->assertEmpty($queries, 'The JOIN count and paginated rows should both hit cache.');
    }

    public function test_join_with_depends_on_paginate_count_invalidates_on_dep_model_version_bump(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->paginate(10);

        $firstKeys = $this->redisKeys('test:count:*');

        Post::create(['title' => 'World', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->paginate(10);

        $secondKeys = $this->redisKeys('test:count:*');

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

    public function test_join_count_with_depends_on_caches_as_scalar_result(): void
    {
        $this->seedAuthorsWithPosts();

        $first = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->count();

        $this->assertSame(3, $first);

        DB::enableQueryLog();
        DB::flushQueryLog();

        $second = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->count();

        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame(3, $second);
        $this->assertEmpty($queries);
        $this->assertNotEmpty($this->redisKeys('test:count:*'));
    }

    public function test_distinct_count_with_depends_on_caches_as_scalar_result(): void
    {
        $this->seedAuthorsWithPosts();

        $first = Post::query()
            ->distinct()
            ->dependsOn([Post::class])
            ->count('author_id');

        $this->assertSame(2, $first);

        DB::enableQueryLog();
        DB::flushQueryLog();

        $second = Post::query()
            ->distinct()
            ->dependsOn([Post::class])
            ->count('author_id');

        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame(2, $second);
        $this->assertEmpty($queries);
    }

    public function test_locked_count_with_depends_on_still_bypasses_scalar_cache(): void
    {
        $this->seedAuthorsWithPosts();

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->count();

        DB::enableQueryLog();
        DB::flushQueryLog();

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->lockForUpdate()
            ->dependsOn([Post::class])
            ->count();

        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotEmpty($queries);
    }

    public function test_from_subquery_with_depends_on_caches_as_blob(): void
    {
        Author::create(['name' => 'Alice']);

        Author::fromSub(Author::query()->select('id', 'name'), 'authors')
            ->dependsOn([Author::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'));
    }

    public function test_raw_order_with_depends_on_can_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()
            ->orderByRaw('CASE WHEN name = ? THEN 0 ELSE 1 END', ['Alice'])
            ->dependsOn([Author::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'));
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

        $this->assertNotEmpty($this->redisKeys('test:result:*'));
    }

    public function test_distinct_with_depends_on_preserves_distinct_semantics(): void
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

    public function test_lock_for_update_with_depends_on_hits_the_db(): void
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

    public function test_aggregate_columns_fall_through_to_db_with_depends_on(): void
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

    public function test_complex_aggregate_with_explicit_dependencies_uses_result_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Author::withCount([
            'posts' => fn($q) => $q->whereRaw('1=1'),
        ])->dependsOn([Post::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'), 'Complex aggregate with dependsOn should use result cache');
    }

    public function test_scalar_count_with_depends_on_caches_as_result(): void
    {
        Author::create(['name' => 'Alice']);

        Author::where('name', 'Alice')->dependsOn([Post::class])->count();

        $this->assertNotEmpty($this->redisKeys('test:count:*'), 'Scalar count with dependsOn should use count namespace');
    }

    // tag() — manual flush grouping

    public function test_tag_is_embedded_in_computed_key(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*:homepage:*'));
        $this->assertEmpty($this->redisKeys('test:result:*[^:]homepage*'));
    }

    public function test_tagged_keys_are_isolated_from_untagged_keys(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();
        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

        $all = $this->redisKeys('test:result:*');
        $tagged = $this->redisKeys('test:result:*:homepage:*');

        $this->assertCount(2, $all);
        $this->assertCount(1, $tagged);
    }

    public function test_flush_tag_removes_only_matching_keys(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();
        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

        $removed = NormCache::flushTag(Author::class, 'homepage');

        $this->assertSame(1, $removed);
        $this->assertNotEmpty($this->redisKeys('test:result:*'));
        $this->assertEmpty($this->redisKeys('test:result:*:homepage:*'));
    }

    public function test_flush_tag_across_models_removes_all_matching(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->tag('deploy')->get();
        Post::query()->dependsOn([Author::class])->tag('deploy')->get();

        $removed = NormCache::flushTagAcrossModels('deploy');

        $this->assertSame(2, $removed);
        $this->assertEmpty($this->redisKeys('test:result:*:deploy:*'));
    }

    public function test_flush_tag_removes_tagged_paginate_count_key(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->paginate(10);

        $this->assertNotEmpty($this->redisKeys('test:count:*:homepage:*'));

        $removed = NormCache::flushTag(Author::class, 'homepage');

        $this->assertGreaterThan(0, $removed);
        $this->assertEmpty($this->redisKeys('test:count:*:homepage:*'));
    }

    public function test_tagged_result_cache_invalidates_on_dep_version_bump(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id, 'published' => true]);

        $first = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->tag('homepage')
            ->get();
        $this->assertCount(1, $first);

        $post->update(['published' => false]);

        $second = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->tag('homepage')
            ->get();
        $this->assertCount(0, $second);
    }

    // Projection isolation

    public function test_depends_on_queries_differing_only_in_select_use_separate_cache_keys(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->get();

        // SELECT clause is part of the cache key — a narrower projection must not return the full-column blob.
        $projected = Author::whereHas('posts')->dependsOn([Post::class])->select('id')->get();

        $this->assertArrayNotHasKey('name', $projected->first()->getAttributes());
    }

    public function test_depends_on_warm_hit_preserves_projected_columns(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->dependsOn([Post::class])->select('id')->get();

        $cached = Author::whereHas('posts')->dependsOn([Post::class])->select('id')->get();

        $this->assertArrayNotHasKey('name', $cached->first()->getAttributes());
    }

    // Helpers

    private function seedAuthorsWithPosts(): void
    {
        $a = Author::create(['name' => 'A']);
        $b = Author::create(['name' => 'B']);
        Post::create(['title' => 'p1', 'author_id' => $a->id, 'views' => 10, 'published' => true]);
        Post::create(['title' => 'p2', 'author_id' => $a->id, 'views' => 20, 'published' => false]);
        Post::create(['title' => 'p3', 'author_id' => $b->id, 'views' => 30, 'published' => true]);
    }

    public function test_join_with_depends_on_and_explicit_select_does_not_collide_with_joined_id(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Earlier', 'author_id' => $author->id]);
        $post = Post::create(['title' => 'Target', 'author_id' => $author->id]);

        $result = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->first();

        $this->assertSame($author->id, $result->id);
        $this->assertNotSame($post->id, $result->id);
    }

    public function test_where_raw_cross_table_dependency_is_not_cached_as_normal_model_query(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $post = Post::create([
            'title' => 'Hello',
            'author_id' => $author->id,
            'published' => true,
        ]);

        $comment = Comment::create([
            'body' => 'Looks good',
            'commentable_type' => Post::class,
            'commentable_id' => $post->id,
        ]);

        $sql = <<<'SQL'
            exists (
                select 1
                from comments
                where comments.commentable_id = posts.id
                and comments.commentable_type = ?
            )
        SQL;

        $first = Post::whereRaw($sql, [Post::class])->get();

        $this->assertCount(1, $first);
        $this->assertTrue($first->first()->is($post));

        $comment->delete();

        $second = Post::whereRaw($sql, [Post::class])->get();

        $this->assertCount(0, $second);
    }
}
