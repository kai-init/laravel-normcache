<?php

namespace NormCache\Tests\Unit;

use NormCache\Payload\MembershipCodec;
use NormCache\Payload\NativeRowAdapter;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheSerializer;
use NormCache\Tests\UnitTestCase;
use stdClass;

final class PayloadCodecTest extends UnitTestCase
{
    public function test_native_rows_round_trip_with_order_types_and_fresh_objects(): void
    {
        $row = new stdClass;
        $row->id = 7;
        $row->numeric = '007';
        $row->binary = "\x00\xff";
        $row->nullable = null;

        $adapter = new NativeRowAdapter;
        $stored = $adapter->toArray($row);
        $first = $adapter->toObject($stored);
        $second = $adapter->toObject($stored);

        $this->assertSame(['id', 'numeric', 'binary', 'nullable'], array_keys($stored));
        $this->assertSame(7, $first->id);
        $this->assertSame('007', $first->numeric);
        $this->assertSame("\x00\xff", $first->binary);
        $this->assertNull($first->nullable);
        $this->assertNotSame($first, $second);
    }

    public function test_raw_result_codec_validates_format_four_envelopes(): void
    {
        $codec = new RawResultCodec(new CacheSerializer, new NativeRowAdapter);
        $row = (object) ['id' => 1, 'amount' => '01.20'];

        $encoded = $codec->encode([$row], '7', ['dep' => '12'], '3');
        $decoded = $codec->decode($encoded);

        $this->assertTrue($decoded->valid);
        $this->assertSame('7', $decoded->epoch);
        $this->assertSame(['dep' => '12'], $decoded->versions);
        $this->assertSame('3', $decoded->tagVersion);
        $this->assertSame('01.20', $decoded->rows[0]->amount);
        $this->assertFalse($codec->decode('not-a-payload')->valid);
    }

    public function test_membership_codec_preserves_order_duplicates_and_string_counters(): void
    {
        $codec = new MembershipCodec;
        $encoded = $codec->encode(
            epoch: '7',
            generation: '4',
            ids: ['i:42', 'i:7', 'i:42'],
            versions: ['b' => '2', 'a' => '1'],
            tagVersion: '3',
        );
        $decoded = $codec->decode($encoded);

        $this->assertTrue($decoded->valid);
        $this->assertSame(['i:42', 'i:7', 'i:42'], $decoded->ids);
        $this->assertSame(['a' => '1', 'b' => '2'], $decoded->versions);
        $this->assertSame('7', $decoded->epoch);
        $this->assertSame('4', $decoded->generation);
        $this->assertSame('3', $decoded->tagVersion);
        $this->assertFalse($codec->decode('{"f":3}')->valid);
    }
}
