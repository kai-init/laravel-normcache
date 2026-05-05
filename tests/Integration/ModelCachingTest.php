<?php

namespace NormCache\Tests\Integration;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedAuthor;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;
use stdClass;

class ModelCachingTest extends TestCase
{
    public function test_querying_models_populates_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::all();

        $this->assertNotEmpty($this->redisKeys('test:*'));
    }

    public function test_get_returns_same_results_as_uncached_baseline(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $cached = Author::all()->pluck('name')->sort()->values();
        $live = UncachedAuthor::all()->pluck('name')->sort()->values();

        $this->assertEquals($live, $cached);
    }

    public function test_creating_model_invalidates_version(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::create(['name' => 'Alice']);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_updating_model_flushes_model_key_and_increments_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);
        $modelKey = NormCache::modelKey(Author::class, $author->id);

        $this->assertNotNull(NormCache::get($modelKey));

        $author->update(['name' => 'Alicia']);

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_deleting_model_flushes_model_key_and_increments_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);
        $modelKey = NormCache::modelKey(Author::class, $author->id);

        $this->assertNotNull(NormCache::get($modelKey));

        $author->delete();

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_soft_deleting_post_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $post->delete();

        $fromCache = Post::all()->pluck('id');
        $fromDb = UncachedPost::all()->pluck('id');

        $this->assertEquals($fromDb, $fromCache);
    }

    public function test_restoring_soft_deleted_model_invalidates_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $post->delete();

        $versionBefore = NormCache::currentVersion(Post::class);

        $post->restore();

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Post::class));
    }

    public function test_force_deleting_soft_deletable_model_flushes_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();

        $versionBefore = NormCache::currentVersion(Post::class);
        $modelKey = NormCache::modelKey(Post::class, $post->id);

        $this->assertNotNull(NormCache::get($modelKey));

        $post->forceDelete();

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Post::class));
    }

    public function test_restoring_soft_deleted_model_serves_fresh_data(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $post->delete();
        Post::all(); // re-warm cache with post excluded

        $post->restore();

        $fromCache = Post::all()->pluck('id');
        $fromDb = UncachedPost::all()->pluck('id');

        $this->assertEquals($fromDb, $fromCache);
        $this->assertContains($post->id, $fromCache);
    }

    public function test_cache_disabled_globally_skips_caching(): void
    {
        $this->app['config']->set('normcache.enabled', false);

        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_flush_command_without_model_flushes_all_keys(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotEmpty($this->redisKeys('test:*'));

        $this->artisan('normcache:flush')->assertSuccessful();

        $this->assertEmpty($this->redisKeys('test:*'));
    }

    public function test_flush_command_with_model_flushes_only_that_model(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Post::all();

        $this->artisan('normcache:flush', ['--model' => Author::class])->assertSuccessful();

        $this->assertEmpty($this->redisKeys('test:model:author:*'));
        $this->assertNotEmpty($this->redisKeys('test:model:post:*'));
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
