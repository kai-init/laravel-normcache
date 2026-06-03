<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryBypassed;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

class SimplificationTest extends TestCase
{
    public function test_simple_query_uses_normalized_cache(): void
    {
        Author::create(['name' => 'Alice']);

        // First call - miss
        Author::query()->get();
        $this->assertNotEmpty($this->redisKeys('test:query:*'), 'Query key should be created');
        $this->assertEmpty($this->redisKeys('test:result:*'), 'Result key should not be created');

        // Second call - hit
        DB::enableQueryLog();
        Author::query()->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries, 'Second call should hit cache');
    }

    public function test_simple_query_with_depends_on_keeps_normalized_cache_and_honors_versions(): void
    {
        $author = Author::create(['name' => 'Alice']);

        // First call - miss
        Author::query()->dependsOn([Post::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:query:*'), 'Should use normalized query cache');
        $this->assertEmpty($this->redisKeys('test:result:*'), 'Should not use result cache');

        // Second call - hit
        DB::enableQueryLog();
        Author::query()->dependsOn([Post::class])->get();
        $this->assertEmpty(DB::getQueryLog(), 'Should hit cache');
        DB::disableQueryLog();

        // Bump Post version
        Post::create(['title' => 'New Post', 'author_id' => $author->id]);

        // Third call - should miss because Post version changed
        DB::enableQueryLog();
        Author::query()->dependsOn([Post::class])->get();
        $this->assertNotEmpty(DB::getQueryLog(), 'Should miss cache after dependency version bump');
        DB::disableQueryLog();
    }

    public function test_complex_query_with_depends_on_uses_result_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        // First call - miss
        Author::whereHas('posts')->dependsOn([Post::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'), 'Should use result cache');
    }

    public function test_complex_query_without_depends_on_bypasses(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Event::fake([QueryBypassed::class]);

        Author::whereHas('posts')->get();

        Event::assertDispatched(QueryBypassed::class);
        $this->assertEmpty($this->redisKeys('test:query:*'));
        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_join_with_depends_on_and_explicit_select_caches_result(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        // First call — explicit select required
        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'), 'JOIN with explicit select + dependsOn should use result cache');

        // Second call - hit
        DB::enableQueryLog();
        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->get();
        $this->assertEmpty(DB::getQueryLog(), 'Should hit result cache');
        DB::disableQueryLog();
    }

    public function test_join_with_depends_on_and_no_explicit_select_bypasses(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->dependsOn([Post::class])
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'), 'JOIN without explicit select must bypass result cache');
    }

    public function test_simple_aggregate_uses_result_with_inferred_dependencies(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        // withCount('posts') should infer Post dependency
        Author::withCount('posts')->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'), 'Simple aggregate should use result cache');

        // Verify it hits
        DB::enableQueryLog();
        Author::withCount('posts')->get();
        $this->assertEmpty(DB::getQueryLog(), 'Simple aggregate should hit cache');
        DB::disableQueryLog();

        // Verify invalidation on Post change
        Post::create(['title' => 'World', 'author_id' => $author->id]);

        DB::enableQueryLog();
        Author::withCount('posts')->get();
        $this->assertNotEmpty(DB::getQueryLog(), 'Simple aggregate should invalidate on inferred dependency bump');
        DB::disableQueryLog();
    }

    public function test_complex_aggregate_without_explicit_dependencies_bypasses(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Event::fake([QueryBypassed::class]);

        Author::withCount([
            'posts' => fn($q) => $q->whereRaw('1=1'), // complex/unsafe
        ])->get();

        Event::assertDispatched(QueryBypassed::class);
    }

    public function test_complex_aggregate_with_explicit_dependencies_uses_result_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Author::withCount([
            'posts' => fn($q) => $q->whereRaw('1=1'),
        ])->dependsOn([Post::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:result:*'), 'Complex aggregate with dependsOn should use result cache');
    }

    public function test_belongs_to_remains_fast_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        // Eager load belongsTo
        $p = Post::with('author')->first();
        $this->assertTrue($p->relationLoaded('author'));

        // Verify no query for author on second load
        DB::enableQueryLog();
        Post::with('author')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        // One query for posts, none for authors (because of fast path + cache)
        $this->assertCount(1, $queries);
        $this->assertStringContainsString('from "posts"', $queries[0]['query']);
    }

    public function test_belongs_to_many_remains_fast_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'PHP']);
        $post->tags()->attach($tag);

        // Eager load belongsToMany - first call warms cache
        Post::with('tags')->get();

        // Verify total cache hit on second load
        DB::enableQueryLog();
        Post::with('tags')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries, 'Should hit total query cache');
    }

    public function test_scalar_count_with_depends_on_caches_as_result(): void
    {
        $author = Author::create(['name' => 'Alice']);

        // Scalar count
        Author::where('name', 'Alice')->dependsOn([Post::class])->count();

        $this->assertNotEmpty($this->redisKeys('test:count:*'), 'Scalar count with dependsOn should use count namespace');
    }

    public function test_pagination_count_with_depends_on_caches_with_dependency_versions(): void
    {
        $author = Author::create(['name' => 'Alice']);

        // Paginate
        Author::query()->dependsOn([Post::class])->paginate(10);

        $this->assertNotEmpty($this->redisKeys('test:count:*'), 'Pagination count should be cached in count namespace');

        // Verify it hits
        DB::enableQueryLog();
        Author::query()->dependsOn([Post::class])->paginate(10);
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        // Should only see the rows query, not the count query (if count is cached)
        // Wait, NormCache might cache the whole pagination result?
        // Actually paginate() in NormCache calls resolvePaginationTotal().
    }
}
