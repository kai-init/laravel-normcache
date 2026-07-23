<?php

namespace NormCache\Tests\Integration\Infrastructure;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;

/**
 * Covers caching correctness for models with non-integer (string/UUID) primary keys.
 */
class StringPrimaryKeyTest extends TestCase
{
    // String / UUID primary keys

    public function test_find_by_string_pk_hits_model_cache_on_second_call(): void
    {
        $id = 'aaaaaaaa-0000-0000-0000-000000000001';
        UuidItem::create(['id' => $id, 'name' => 'Alpha']);

        // Warm the cache.
        UuidItem::find($id);

        $this->assertNotNull($this->modelCacheEntry(UuidItem::class, $id));
    }

    public function test_get_with_string_pk_returns_correct_models(): void
    {
        $id1 = 'aaaaaaaa-0000-0000-0000-000000000001';
        $id2 = 'bbbbbbbb-0000-0000-0000-000000000002';
        UuidItem::create(['id' => $id1, 'name' => 'Alpha']);
        UuidItem::create(['id' => $id2, 'name' => 'Beta']);

        $names = UuidItem::orderBy('name')->get()->pluck('name')->all();

        $this->assertSame(['Alpha', 'Beta'], $names);

        // Second call hits the query and model caches.
        $namesCached = UuidItem::orderBy('name')->get()->pluck('name')->all();

        $this->assertSame($names, $namesCached);
    }

    public function test_update_with_string_pk_evicts_model_cache(): void
    {
        $id = 'aaaaaaaa-0000-0000-0000-000000000001';
        $item = UuidItem::create(['id' => $id, 'name' => 'Alpha']);

        // Warm the model cache.
        UuidItem::find($id);
        $this->assertNotNull($this->modelCacheEntry(UuidItem::class, $id));

        $item->update(['name' => 'AlphaUpdated']);

        $this->assertNull($this->modelCacheEntry(UuidItem::class, $id));
    }

    public function test_delete_with_string_pk_evicts_model_cache_and_bumps_version(): void
    {
        $id = 'aaaaaaaa-0000-0000-0000-000000000001';
        $item = UuidItem::create(['id' => $id, 'name' => 'Alpha']);

        UuidItem::find($id);
        $this->assertNotNull($this->modelCacheEntry(UuidItem::class, $id));

        $versionBefore = NormCache::currentVersion(UuidItem::class);

        $item->delete();

        $this->assertNull($this->modelCacheEntry(UuidItem::class, $id));
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(UuidItem::class));
    }

    public function test_query_result_with_string_pks_is_fresh_after_insert(): void
    {
        $id1 = 'aaaaaaaa-0000-0000-0000-000000000001';
        UuidItem::create(['id' => $id1, 'name' => 'Alpha']);

        // Cache the result.
        $first = UuidItem::orderBy('name')->get()->pluck('name')->all();
        $this->assertSame(['Alpha'], $first);

        // Insert a new item — version is bumped.
        $id2 = 'bbbbbbbb-0000-0000-0000-000000000002';
        UuidItem::create(['id' => $id2, 'name' => 'Beta']);

        // Must reflect the new item.
        $second = UuidItem::orderBy('name')->get()->pluck('name')->all();
        $this->assertSame(['Alpha', 'Beta'], $second);
    }

    public function test_where_in_with_multiple_string_pks_returns_correct_models(): void
    {
        $id1 = 'aaaaaaaa-0000-0000-0000-000000000001';
        $id2 = 'bbbbbbbb-0000-0000-0000-000000000002';
        $id3 = 'cccccccc-0000-0000-0000-000000000003';

        UuidItem::create(['id' => $id1, 'name' => 'Alpha']);
        UuidItem::create(['id' => $id2, 'name' => 'Beta']);
        UuidItem::create(['id' => $id3, 'name' => 'Gamma']);

        $names = UuidItem::whereIn('id', [$id1, $id3])->orderBy('name')->get()->pluck('name')->all();

        $this->assertSame(['Alpha', 'Gamma'], $names);

        // Second call — from cache.
        $namesCached = UuidItem::whereIn('id', [$id1, $id3])->orderBy('name')->get()->pluck('name')->all();

        $this->assertSame($names, $namesCached);
    }
}
