<?php

namespace NormCache\Tests\Unit\Payload;

use NormCache\Payload\ThroughIndexAdapter;
use NormCache\Tests\UnitTestCase;

class ThroughIndexAdapterTest extends UnitTestCase
{
    public function test_it_normalizes_aligned_ids_and_preserves_through_keys(): void
    {
        $adapter = new ThroughIndexAdapter;
        $payload = ['ids' => [1, 2], 'throughKeys' => ['through-1', 'through-2']];

        $this->assertSame('{"i":["1","2"],"t":["through-1","through-2"]}', $adapter->encode($payload));
        $this->assertSame([
            'ids' => ['1', '2'],
            'throughKeys' => ['through-1', 'through-2'],
        ], $adapter->decode($adapter->encode($payload))->payload);
    }

    public function test_it_rejects_misaligned_ids_and_through_keys(): void
    {
        $adapter = new ThroughIndexAdapter;

        $this->assertFalse($adapter->decode('{"i":[1,2],"t":["through-1"]}')->valid);
    }
}
