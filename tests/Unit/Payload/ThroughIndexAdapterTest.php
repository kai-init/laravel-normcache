<?php

namespace NormCache\Tests\Unit\Payload;

use NormCache\Payload\ThroughIndexAdapter;
use NormCache\Tests\UnitTestCase;

class ThroughIndexAdapterTest extends UnitTestCase
{
    public function test_cardinality_counts_ids_not_the_compound_payload_keys(): void
    {
        $adapter = new ThroughIndexAdapter;

        $payload = ['ids' => [1, 2, 3], 'throughKeys' => ['a', 'b', 'c']];

        $this->assertSame(3, $adapter->cardinality($payload));
    }

    public function test_cardinality_is_zero_for_an_empty_index(): void
    {
        $adapter = new ThroughIndexAdapter;

        $this->assertSame(0, $adapter->cardinality(['ids' => [], 'throughKeys' => []]));
    }
}
