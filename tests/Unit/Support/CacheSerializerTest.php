<?php

namespace NormCache\Tests\Unit\Support;

use NormCache\Support\CacheSerializer;
use NormCache\Tests\TestCase;

class CacheSerializerTest extends TestCase
{
    private CacheSerializer $serializer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->serializer = new CacheSerializer;
    }

    public function test_integer_roundtrip(): void
    {
        $this->assertSame(42, $this->serializer->unserialize($this->serializer->serialize(42)));
    }

    public function test_float_roundtrip(): void
    {
        $this->assertSame(3.14, $this->serializer->unserialize($this->serializer->serialize(3.14)));
    }

    public function test_array_roundtrip(): void
    {
        $data = ['name' => 'Alice', 'age' => 30];
        $this->assertSame($data, $this->serializer->unserialize($this->serializer->serialize($data)));
    }

    public function test_zero_roundtrip(): void
    {
        $this->assertSame(0, $this->serializer->unserialize($this->serializer->serialize(0)));
    }

    public function test_unserialize_many_handles_null_entries(): void
    {
        $serialized = $this->serializer->serialize(['key' => 'val']);
        $result = $this->serializer->unserializeMany([$serialized, null, false]);

        $this->assertSame(['key' => 'val'], $result[0]);
        $this->assertNull($result[1]);
        $this->assertNull($result[2]);
    }

    public function test_unserialize_numeric_string_returns_int(): void
    {
        $this->assertSame(7, $this->serializer->unserialize('7'));
    }

    public function test_unserialize_float_string_returns_float(): void
    {
        $this->assertSame(1.5, $this->serializer->unserialize('1.5'));
    }
}
