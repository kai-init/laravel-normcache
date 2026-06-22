<?php

namespace NormCache\Tests\Integration\Invalidation;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: soft-delete lifecycle (delete, restore, forceDelete) must
 * correctly invalidate model cache entries and version counters.
 */
class SoftDeleteInvalidationTest extends TestCase
{
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

    public function test_soft_deleting_model_increments_version_by_exactly_one(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Post::all();

        $versionBefore = NormCache::currentVersion(Post::class);

        $post->delete();

        $this->assertSame($versionBefore + 1, NormCache::currentVersion(Post::class));
    }

    public function test_force_deleting_soft_deletable_model_flushes_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();

        $versionBefore = NormCache::currentVersion(Post::class);

        $this->assertNotNull($this->modelCacheEntry(Post::class, $post->id));

        $post->forceDelete();

        $this->assertNull($this->modelCacheEntry(Post::class, $post->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Post::class));
    }

    public function test_restoring_soft_deleted_model_serves_fresh_data(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $post->delete();
        Post::all();

        $post->restore();

        $fromCache = Post::all()->pluck('id');
        $fromDb = UncachedPost::all()->pluck('id');

        $this->assertEquals($fromDb, $fromCache);
        $this->assertContains($post->id, $fromCache);
    }

    public function test_soft_deleted_model_is_not_written_to_model_cache_on_miss(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $this->assertNotNull($this->modelCacheEntry(Post::class, $post->id));

        $post->delete();
        $this->assertNull($this->modelCacheEntry(Post::class, $post->id));

        // Trashed models must not be written to the model cache on a miss; normal queries would then surface them.
        NormCache::getModels([$post->id], Post::class);

        $this->assertNull($this->modelCacheEntry(Post::class, $post->id));
    }

    public function test_soft_deleted_model_is_not_returned_from_model_cache_miss(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $post->delete();

        $this->assertSame([], NormCache::getModels([$post->id], Post::class));
    }

    public function test_with_trashed_query_can_return_soft_deleted_model_after_model_cache_miss(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $post->delete();

        $posts = Post::withTrashed()->whereKey($post->id)->get();

        $this->assertCount(1, $posts);
        $this->assertTrue($posts->first()->trashed());
    }

    public function test_soft_deleted_model_excluded_from_subsequent_cache_reads(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $post->delete();

        // Populate model cache for the deleted ID
        NormCache::getModels([$post->id], Post::class);

        $ids = Post::all()->pluck('id');
        $this->assertNotContains($post->id, $ids);
    }

    public function test_soft_delete_flushes_model_key_correctly(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $this->assertNotNull($this->modelCacheEntry(Post::class, $post->id));

        $post->delete();

        $this->assertNull($this->modelCacheEntry(Post::class, $post->id));
    }

    public function test_version_bumped_after_soft_delete_not_before(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Post::all();
        $versionBefore = NormCache::currentVersion(Post::class);

        $post->delete();

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Post::class));

        $fromCache = Post::all()->pluck('id');
        $this->assertNotContains($post->id, $fromCache);
    }

    public function test_quiet_restore_invalidates_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $post->delete();
        Post::all();

        $versionBefore = NormCache::currentVersion(Post::class);

        $post->restoreQuietly();

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Post::class));
        $this->assertSame([$post->id], Post::all()->pluck('id')->all());
    }

    public function test_bulk_restore_and_force_delete_invalidate_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $post->delete();
        Post::all();

        $versionBeforeRestore = NormCache::currentVersion(Post::class);

        Post::onlyTrashed()->whereKey($post->id)->restore();

        $this->assertGreaterThan($versionBeforeRestore, NormCache::currentVersion(Post::class));

        $versionBeforeForceDelete = NormCache::currentVersion(Post::class);

        Post::whereKey($post->id)->forceDelete();

        $this->assertGreaterThan($versionBeforeForceDelete, NormCache::currentVersion(Post::class));
        $this->assertCount(0, Post::withTrashed()->get());
    }
}
