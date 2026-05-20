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

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->updateOrCreate(['id' => $author->id], ['name' => 'Alicia']);

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_quiet_instance_writes_still_invalidate_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBefore = NormCache::currentVersion(Author::class);

        $author->updateQuietly(['name' => 'Alicia']);

        $this->assertNull(NormCache::get($modelKey));
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

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBefore = NormCache::currentVersion(Author::class);

        $author->deleteQuietly();

        $this->assertNull(NormCache::get($modelKey));
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

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->upsert(
            [['id' => 1, 'name' => 'Alicia', 'created_at' => now(), 'updated_at' => now()]],
            ['id'],
            ['name', 'updated_at']
        );

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
        $this->assertSame('Alicia', Author::first()->name);
    }

    public function test_update_or_insert_invalidates_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBefore = NormCache::currentVersion(Author::class);

        Author::query()->updateOrInsert(
            ['name' => 'Alice'],
            ['name' => 'Alicia', 'updated_at' => now()]
        );

        $this->assertNull(NormCache::get($modelKey));
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

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBeforeTouch = NormCache::currentVersion(Author::class);

        Author::whereKey($author->id)->touch();

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBeforeTouch, NormCache::currentVersion(Author::class));

        Author::find($author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        $versionBeforeIncrement = NormCache::currentVersion(Author::class);

        Author::whereKey($author->id)->incrementEach(['id' => 0]);

        $this->assertNull(NormCache::get($modelKey));
        $this->assertGreaterThan($versionBeforeIncrement, NormCache::currentVersion(Author::class));
    }

    public function test_bulk_update_flushes_all_model_keys(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        $carol = Author::create(['name' => 'Carol']);
        Author::all();

        $aliceKey = NormCache::modelKey(Author::class, $alice->id);
        $bobKey = NormCache::modelKey(Author::class, $bob->id);
        $carolKey = NormCache::modelKey(Author::class, $carol->id);

        Author::whereBetween('id', [$bob->id, $bob->id])->update(['name' => 'Bobby']);

        $this->assertNull(NormCache::get($aliceKey));
        $this->assertNull(NormCache::get($bobKey));
        $this->assertNull(NormCache::get($carolKey));
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

        $key2 = NormCache::modelKey(Author::class, $a2->id);
        $this->assertNotNull(NormCache::get($key2));

        Author::where(fn ($q) => $q->whereIn('id', [$a1->id]))->update(['name' => 'Alicia']);

        $this->assertNull(NormCache::get(NormCache::modelKey(Author::class, $a1->id)));
        $this->assertNull(NormCache::get($key2));
    }

    public function test_direct_where_in_update_flushes_model_cache(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $key2 = NormCache::modelKey(Author::class, $a2->id);
        $this->assertNotNull(NormCache::get($key2));

        Author::whereIn('id', [$a1->id])->update(['name' => 'Alicia']);

        $this->assertNull(NormCache::get($key2));
    }

    public function test_instance_update_only_evicts_that_model_key(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $key1 = NormCache::modelKey(Author::class, $a1->id);
        $key2 = NormCache::modelKey(Author::class, $a2->id);

        $a1->update(['name' => 'Alicia']);

        $this->assertNull(NormCache::get($key1));
        $this->assertNotNull(NormCache::get($key2));
    }

    public function test_instance_increment_only_evicts_that_model_key(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $key1 = NormCache::modelKey(Author::class, $a1->id);
        $key2 = NormCache::modelKey(Author::class, $a2->id);

        $a1->increment('id', 0);

        $this->assertNull(NormCache::get($key1));
        $this->assertNotNull(NormCache::get($key2));
    }

    public function test_instance_decrement_only_evicts_that_model_key(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::all();

        $key1 = NormCache::modelKey(Author::class, $a1->id);
        $key2 = NormCache::modelKey(Author::class, $a2->id);

        $a1->decrement('id', 0);

        $this->assertNull(NormCache::get($key1));
        $this->assertNotNull(NormCache::get($key2));
    }
}
