<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: verifies the cache-mode routing rules — simple queries use
 * normalized cache, complex/dependsOn queries use result cache, and unsupported
 * shapes bypass caching entirely.
 */
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
}
