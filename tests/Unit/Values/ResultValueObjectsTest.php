<?php

namespace NormCache\Tests\Unit\Values;

use NormCache\Enums\CacheStatus;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ResultCacheResult;
use PHPUnit\Framework\TestCase;

class ResultValueObjectsTest extends TestCase
{
    public function test_query_cache_result_exposes_typed_status(): void
    {
        $result = new QueryCacheResult(
            status: CacheStatus::Hit,
            key: 'query:v1:abc',
            ids: [1, 2, 3],
            models: null,
            buildingKey: null,
            buildingToken: null,
            versionKeys: [],
            expectedVersions: [],
        );

        $this->assertSame(CacheStatus::Hit, $result->status);
        $this->assertSame([1, 2, 3], $result->ids);
    }

    public function test_result_cache_result_exposes_typed_status(): void
    {
        $result = new ResultCacheResult(
            status: CacheStatus::Miss,
            key: 'result:v1:abc',
            payload: null,
            buildingKey: 'building:abc',
            buildingToken: 'tok123',
            wakeKey: 'wake:abc',
            versionKeys: ['ver:posts:'],
            expectedVersions: ['5'],
        );

        $this->assertSame(CacheStatus::Miss, $result->status);
        $this->assertSame('building:abc', $result->buildingKey);
        $this->assertSame(['ver:posts:'], $result->versionKeys);
    }

    public function test_pivot_cache_result_missed_ids_returns_non_array_entries(): void
    {
        $result = new PivotCacheResult(
            seg: 'v1',
            data: [1 => [['id' => 5]], 2 => null, 3 => false],
            versionKeys: [],
            expectedVersions: [],
        );

        $this->assertSame([2, 3], $result->missedIds());
    }

    public function test_pivot_cache_result_missed_ids_empty_when_all_present(): void
    {
        $result = new PivotCacheResult(
            seg: 'v1',
            data: [1 => [['id' => 5]], 2 => [['id' => 6]]],
            versionKeys: [],
            expectedVersions: [],
        );

        $this->assertSame([], $result->missedIds());
    }
}
