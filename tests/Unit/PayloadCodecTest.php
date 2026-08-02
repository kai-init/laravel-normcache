<?php

namespace NormCache\Tests\Unit;

use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheSerializer;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;

final class PayloadCodecTest extends UnitTestCase
{
    public function test_raw_result_codec_owns_native_row_conversion(): void
    {
        $row = new \stdClass;
        $row->id = 7;
        $row->numeric = '007';
        $row->binary = "\x00\xff";
        $row->nullable = null;

        $codec = new RawResultCodec(new CacheSerializer);

        $first = $codec->decode($codec->encode([$row], '7'))->rows[0];
        $second = $codec->decode($codec->encode([$row], '7'))->rows[0];

        $this->assertSame(['id', 'numeric', 'binary', 'nullable'], array_keys((array) $first));
        $this->assertSame(7, $first->id);
        $this->assertSame('007', $first->numeric);
        $this->assertSame("\x00\xff", $first->binary);
        $this->assertNull($first->nullable);
        $this->assertNotSame($first, $second);
    }

    public function test_raw_result_codec_validates_envelopes(): void
    {
        $codec = new RawResultCodec(new CacheSerializer);
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

    public function test_row_codec_returns_exactly_one_object_for_every_valid_payload(): void
    {
        $codec = new RawResultCodec(new CacheSerializer);
        $row = (object) ['id' => 7, 'title' => 'Post'];

        $decoded = $codec->decodeRow($codec->encodeRow($row, '9'));

        $this->assertTrue($decoded->valid);
        $this->assertCount(1, $decoded->rows);
        $this->assertInstanceOf(\stdClass::class, $decoded->rows[0]);
        $this->assertSame(7, $decoded->rows[0]->id);
    }

    public function test_row_codec_rejects_a_non_canonical_primary_key_token(): void
    {
        $codec = new RawResultCodec(new CacheSerializer);
        $metadata = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
        $payload = $codec->encodeRow((object) ['id' => 7], '9');

        $this->assertTrue($codec->decodeRow($payload, $metadata, 'i:7')->valid);
        $this->assertFalse($codec->decodeRow($payload, $metadata, 's:7')->valid);
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

    public function test_membership_codec_rejects_invalid_dependency_versions(): void
    {
        $codec = new MembershipCodec;
        $integerVersion = json_encode([
            'f' => 4,
            'ep' => '0',
            'g' => '0',
            'ids' => 'i:1',
            'vec' => ['dependency' => 1],
        ], JSON_THROW_ON_ERROR);

        $this->assertFalse($codec->decode($integerVersion)->valid);
    }

    public function test_membership_codec_round_trips_an_empty_membership(): void
    {
        $codec = new MembershipCodec;

        $this->assertSame([], $codec->decode($codec->encode('7', '4', []))->ids);
    }

    public function test_membership_codec_rejects_the_previous_array_id_layout(): void
    {
        $codec = new MembershipCodec;
        $previous = json_encode([
            'f' => 4,
            'ep' => '7',
            'g' => '4',
            'ids' => ['i:42', 'i:7'],
            'vec' => [],
        ], JSON_THROW_ON_ERROR);

        $this->assertFalse($codec->decode($previous)->valid);
    }
}
