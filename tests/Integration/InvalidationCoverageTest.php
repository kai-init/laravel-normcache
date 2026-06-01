<?php

namespace NormCache\Tests\Integration;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class InvalidationCoverageTest extends TestCase
{
    public function test_first_or_create_invalidates_when_creating(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->firstOrCreate(['name' => 'Alice']);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_update_or_create_invalidates_existing_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->updateOrCreate(['id' => $author->id], ['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_quiet_instance_writes_still_invalidate_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = NormCache::currentVersion(Author::class);

        $author->updateQuietly(['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_quiet_create_invalidates_query_cache(): void
    {
        Author::all();
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->createQuietly(['name' => 'Alice']);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertSame(['Alice'], Author::all()->pluck('name')->all());
    }

    public function test_quiet_delete_invalidates_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = NormCache::currentVersion(Author::class);

        $author->deleteQuietly();

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
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

    public function test_upsert_invalidates_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->upsert(
            [['id' => 1, 'name' => 'Alicia', 'created_at' => now(), 'updated_at' => now()]],
            ['id'],
            ['name', 'updated_at']
        );

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertSame('Alicia', Author::first()->name);
    }

    public function test_update_or_insert_invalidates_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->updateOrInsert(
            ['name' => 'Alice'],
            ['name' => 'Alicia', 'updated_at' => now()]
        );

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertSame('Alicia', Author::first()->name);
    }

    public function test_insert_or_ignore_invalidates_query_cache(): void
    {
        Author::all();
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->insertOrIgnore([
            'name' => 'Alice',
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertSame(['Alice'], Author::all()->pluck('name')->all());
    }

    public function test_insert_using_invalidates_query_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->insertUsing(
            ['name', 'created_at', 'updated_at'],
            Author::query()->select('name', 'created_at', 'updated_at')->where('name', 'Alice')
        );

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertCount(2, Author::all());
    }

    public function test_touch_and_increment_each_flush_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBeforeTouch = NormCache::currentVersion(Author::class);

        Author::whereKey($author->id)->touch();

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBeforeTouch, NormCache::currentVersion(Author::class));

        Author::find($author->id);
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBeforeIncrement = NormCache::currentVersion(Author::class);

        Author::whereKey($author->id)->incrementEach(['id' => 0]);

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBeforeIncrement, NormCache::currentVersion(Author::class));
    }

    public function test_bulk_update_flushes_all_model_keys(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        $carol = Author::create(['name' => 'Carol']);
        Author::all();

        Author::whereBetween('id', [$bob->id, $bob->id])->update(['name' => 'Bobby']);

        $this->assertNull($this->modelCacheEntry(Author::class, $alice->id));
        $this->assertNull($this->modelCacheEntry(Author::class, $bob->id));
        $this->assertNull($this->modelCacheEntry(Author::class, $carol->id));
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

    public function test_grouped_where_update_flushes_model_cache(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $a2->id));

        Author::where(fn($q) => $q->whereIn('id', [$a1->id]))->update(['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $a1->id));
        $this->assertNull($this->modelCacheEntry(Author::class, $a2->id));
    }

    public function test_direct_where_in_update_flushes_model_cache(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $a2->id));

        Author::whereIn('id', [$a1->id])->update(['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $a2->id));
    }

    public function test_instance_update_only_evicts_that_model_key(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $a1->update(['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $a1->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $a2->id));
    }

    public function test_instance_increment_only_evicts_that_model_key(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $a1->increment('id', 0);

        $this->assertNull($this->modelCacheEntry(Author::class, $a1->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $a2->id));
    }

    public function test_instance_decrement_only_evicts_that_model_key(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $a1->decrement('id', 0);

        $this->assertNull($this->modelCacheEntry(Author::class, $a1->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $a2->id));
    }

    public function test_new_query_from_existing_instance_update_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $versionBefore = NormCache::currentVersion(Author::class);

        // newQuery() on a live instance sets $this->model->exists = true, which takes the instance-flush path
        $author->newQuery()->where('id', $author->id)->update(['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_save_invalidates_when_saving_listener_makes_a_clean_model_dirty(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::find($author->id);

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        Author::saving(function (Author $model) {
            if ($model->name === 'Alice') {
                $model->name = 'Alicia';
            }
        });

        try {
            $clean = Author::find($author->id);

            $this->assertTrue($clean->save());
            $this->assertSame('Alicia', Author::withoutCache()->find($author->id)->name);
            $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
            $this->assertSame('Alicia', Author::find($author->id)->name);
        } finally {
            Author::flushEventListeners();
        }
    }

    public function test_dirty_existing_model_save_only_invalidates_once_outside_transaction(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $post = Post::create([
            'title' => 'Original',
            'author_id' => $author->id,
            'published' => true,
        ]);

        // Reset the version after setup so the create invalidation does not matter.
        NormCache::flushModel(Post::class);

        $before = NormCache::currentVersion(Post::class);

        $post->title = 'Changed';
        $post->save();

        $after = NormCache::currentVersion(Post::class);

        $this->assertSame(
            $before + 1,
            $after,
            'Dirty existing model save should invalidate the model version exactly once.'
        );
    }

    public function test_dirty_existing_model_save_quietly_only_invalidates_once_outside_transaction(): void
    {
        config()->set('normcache.cooldown', 0);

        $author = Author::create(['name' => 'Alice']);

        $post = Post::create([
            'title' => 'Original',
            'author_id' => $author->id,
            'published' => true,
        ]);

        $before = NormCache::currentVersion(Post::class);

        $post->title = 'Changed';
        $post->saveQuietly();

        $after = NormCache::currentVersion(Post::class);

        $this->assertSame(
            $before + 1,
            $after,
            'Dirty existing model saveQuietly should invalidate the model version exactly once.'
        );
    }

    public function test_clean_existing_model_save_does_not_invalidate(): void
    {
        config()->set('normcache.cooldown', 0);

        $author = Author::create(['name' => 'Alice']);

        $post = Post::create([
            'title' => 'Original',
            'author_id' => $author->id,
            'published' => true,
        ]);

        $before = NormCache::currentVersion(Post::class);

        $post->save();

        $after = NormCache::currentVersion(Post::class);

        $this->assertSame($before, $after);
    }

    public function test_builder_update_still_invalidates_once(): void
    {
        config()->set('normcache.cooldown', 0);

        $author = Author::create(['name' => 'Alice']);

        Post::create([
            'title' => 'Original',
            'author_id' => $author->id,
            'published' => false,
        ]);

        $before = NormCache::currentVersion(Post::class);

        Post::where('published', false)->update(['published' => true]);

        $after = NormCache::currentVersion(Post::class);

        $this->assertSame($before + 1, $after);
    }

    public function test_model_create_invalidates_once(): void
    {
        config()->set('normcache.cooldown', 0);

        $author = Author::create(['name' => 'Alice']);

        $before = NormCache::currentVersion(Post::class);

        Post::create([
            'title' => 'Created',
            'author_id' => $author->id,
            'published' => true,
        ]);

        $after = NormCache::currentVersion(Post::class);

        $this->assertSame(
            $before + 1,
            $after,
            'Model create should invalidate the model version exactly once.'
        );
    }

    public function test_model_restore_invalidates_once(): void
    {
        config()->set('normcache.cooldown', 0);

        $author = Author::create(['name' => 'Alice']);

        $post = Post::create([
            'title' => 'Original',
            'author_id' => $author->id,
            'published' => true,
        ]);

        $post->delete();

        $before = NormCache::currentVersion(Post::class);

        $post->restore();

        $after = NormCache::currentVersion(Post::class);

        $this->assertSame($before + 1, $after);
    }
}
