<?php

namespace NormCache\Tests\Integration\Invalidation;

use Illuminate\Support\Collection;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: builder-level write operations (firstOrCreate, updateOrCreate,
 * upsert, updateOrInsert, insertOrIgnore, insertUsing, bulk update/restore) must
 * correctly invalidate model cache entries and version counters.
 */
class BuilderInvalidationTest extends TestCase
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

    public function test_insert_or_ignore_returning_uses_the_query_result_and_skips_noop_conflicts(): void
    {
        if (!method_exists(Author::query()->toBase(), 'insertOrIgnoreReturning')) {
            $this->markTestSkipped('insertOrIgnoreReturning is available in Laravel 13 and later.');
        }

        Author::all();
        $versionBeforeInsert = NormCache::currentVersion(Author::class);

        $inserted = Author::query()->insertOrIgnoreReturning([
            'id' => 1,
            'name' => 'Alice',
            'created_at' => now(),
            'updated_at' => now(),
        ], ['id', 'name'], ['id']);

        $this->assertInstanceOf(Collection::class, $inserted);
        $this->assertSame([['id' => 1, 'name' => 'Alice']], $inserted->map(fn($row) => (array) $row)->all());
        $this->assertGreaterThan($versionBeforeInsert, NormCache::currentVersion(Author::class));

        $versionBeforeConflict = NormCache::currentVersion(Author::class);
        $ignored = Author::query()->insertOrIgnoreReturning([
            'id' => 1,
            'name' => 'Ignored',
            'created_at' => now(),
            'updated_at' => now(),
        ], ['id', 'name'], ['id']);

        $this->assertTrue($ignored->isEmpty());
        $this->assertSame($versionBeforeConflict, NormCache::currentVersion(Author::class));
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

        $touched = Author::whereKey($author->id)->touch();

        $this->assertSame(1, $touched);
        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertGreaterThan($versionBeforeTouch, NormCache::currentVersion(Author::class));

        Author::find($author->id);
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $versionBeforeIncrement = NormCache::currentVersion(Author::class);

        $affected = Author::whereKey($author->id)->incrementEach(['id' => 0]);

        $this->assertSame(1, $affected);
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
}
