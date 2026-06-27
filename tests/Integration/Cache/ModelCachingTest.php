<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: model entities are written to cache on query, returned correctly
 * on warm hits, and cleared by flush commands (global and per-model).
 */
class ModelCachingTest extends TestCase
{
    public function test_cache_disabled_globally_skips_caching(): void
    {
        $this->app['config']->set('normcache.enabled', false);

        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertEmpty($this->redisKeys('query:*'));
    }

    public function test_flush_command_without_model_flushes_all_keys(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotEmpty($this->redisKeys('*'));

        $this->artisan('normcache:flush')->assertSuccessful();

        $this->assertEmpty($this->redisKeys('*'));
    }

    public function test_flush_command_with_model_flushes_only_that_model(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Post::all();

        $this->artisan('normcache:flush', ['--model' => Author::class])->assertSuccessful();

        // Author entries are unreachable after the version bump; Post cache is unaffected.
        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertNotNull($this->modelCacheEntry(Post::class, $post->id));
    }

    public function test_flush_command_rejects_nonexistent_class(): void
    {
        $this->artisan('normcache:flush', ['--model' => 'App\\Models\\DoesNotExist'])
            ->assertFailed();
    }

    public function test_flush_command_rejects_class_without_cacheable_trait(): void
    {
        $this->artisan('normcache:flush', ['--model' => stdClass::class])
            ->assertFailed();
    }
}
