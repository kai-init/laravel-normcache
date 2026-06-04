<?php

namespace NormCache\Tests\Integration\Invalidation;

use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: model-lifecycle events (save, delete, increment, quiet
 * variants, withoutEvents) must correctly invalidate model cache entries and
 * version counters.
 */
class ModelInvalidationTest extends TestCase
{
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

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $author->update(['name' => 'Alicia']);

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_deleting_model_flushes_model_key_and_increments_version(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $author->delete();

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_members_set_has_ttl_matching_model_ttl(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $classKey = NormCache::classKey(Author::class);
        $memberKey = 'test:members:model:{' . $classKey . '}';
        $redis = Redis::connection('model-cache-test');

        $this->assertGreaterThan(0, $redis->ttl($memberKey), 'members:model: set must have a TTL');
    }

    public function test_members_set_dead_keys_are_bounded_by_ttl(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $classKey = NormCache::classKey(Author::class);
        $memberKey = 'test:members:model:{' . $classKey . '}';
        $modelKey = $this->prefixedModelKey(Author::class, $author->id);
        $redis = Redis::connection('model-cache-test');

        // Simulate model key expiry by deleting it directly.
        $redis->del($modelKey);
        $this->assertFalse((bool) $redis->exists($modelKey));

        // Dead keys can accumulate in the members set, but the set's own TTL bounds that growth.
        $this->assertGreaterThan(0, $redis->ttl($memberKey), 'members set must expire, bounding dead-key accumulation');
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

    public function test_model_save_inside_without_events_still_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::withoutEvents(function () use ($author) {
            $author->name = 'Alicia';
            $author->save();
        });

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_model_create_inside_without_events_still_bumps_version(): void
    {
        Author::all();
        $versionBefore = NormCache::currentVersion(Author::class);

        Author::withoutEvents(fn() => Author::create(['name' => 'Alice']));

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertSame(['Alice'], Author::all()->pluck('name')->all());
    }

    public function test_touching_belongs_to_relation_invalidates_related_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::all();
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $versionBefore = NormCache::currentVersion(Author::class);

        $post->author()->touch();

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
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
}
