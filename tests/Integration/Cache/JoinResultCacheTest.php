<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
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
        DB::listen(fn() => $queryCount++);

        $results = Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->get();

        $this->assertSame(0, $queryCount);
        $this->assertCount(1, $results);
        $this->assertSame('Alice', $results->first()->name);
    }

    public function test_join_without_depends_on_bypasses_regardless_of_select(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::query()
            ->join('posts', 'posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->get();

        $this->assertEmpty($this->redisKeys('test:result:*'));
        $this->assertEmpty($this->redisKeys('test:query:*'));
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
}
