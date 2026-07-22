<?php

namespace NormCache\Tests\Unit\Values;

use NormCache\Values\PivotCacheResult;
use PHPUnit\Framework\TestCase;

final class PivotCacheResultTest extends TestCase
{
    public function test_missed_ids_returns_non_array_entries(): void
    {
        $result = new PivotCacheResult(
            seg: 'v1',
            data: [1 => [['id' => 5]], 2 => null, 3 => false],
        );

        $this->assertSame([2, 3], $result->missedIds());
    }

    public function test_missed_ids_is_empty_when_all_entries_are_present(): void
    {
        $result = new PivotCacheResult(
            seg: 'v1',
            data: [1 => [['id' => 5]], 2 => [['id' => 6]]],
        );

        $this->assertSame([], $result->missedIds());
    }
}
