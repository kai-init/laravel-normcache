<?php

namespace NormCache\Tests\Unit;

use NormCache\Payload\ChangeRecordCodec;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheSerializer;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;
use PHPUnit\Framework\Attributes\DataProvider;

final class PayloadCodecTest extends UnitTestCase
{
    public function test_change_record_codec_round_trips(): void
    {
        $codec = new ChangeRecordCodec;

        $record = $codec->decode($codec->encode('update', ['views', 'title'], true));

        $this->assertTrue($record->valid);
        $this->assertSame('update', $record->mutation);
        $this->assertSame(['title', 'views'], $record->columns);
        $this->assertTrue($record->precise);
    }

    public function test_change_record_codec_round_trips_an_empty_column_list(): void
    {
        $codec = new ChangeRecordCodec;

        $record = $codec->decode($codec->encode('update', [], true));

        $this->assertTrue($record->valid);
        $this->assertSame([], $record->columns);
    }

    #[DataProvider('malformedChangeRecords')]
    public function test_change_record_codec_rejects_malformed_payloads(string $payload): void
    {
        $this->assertFalse((new ChangeRecordCodec)->decode($payload)->valid);
    }

    /** @return iterable<string, array{string}> */
    public static function malformedChangeRecords(): iterable
    {
        yield 'not json' => ['not-a-payload'];
        yield 'not an object' => ['[1,2,3]'];
        yield 'unknown format' => ['{"f":2,"m":"update","c":[],"p":true}'];
        yield 'missing format' => ['{"m":"update","c":[],"p":true}'];
        yield 'string format' => ['{"f":"1","m":"update","c":[],"p":true}'];
        yield 'missing mutation' => ['{"f":1,"c":[],"p":true}'];
        yield 'columns not a list' => ['{"f":1,"m":"update","c":"title","p":true}'];
        yield 'non string column' => ['{"f":1,"m":"update","c":[7],"p":true}'];
        yield 'precise not a bool' => ['{"f":1,"m":"update","c":[],"p":1}'];
        yield 'missing precise' => ['{"f":1,"m":"update","c":[]}'];
    }

    public function test_raw_result_codec_owns_native_row_conversion(): void
    {
        $row = new \stdClass;
        $row->id = 7;
        $row->numeric = '007';
        $row->binary = "\x00\xff";
        $row->nullable = null;

        $codec = new RawResultCodec(new CacheSerializer);

        $first = $codec->decode($codec->encode([$row], '7', '1'))->rows[0];
        $second = $codec->decode($codec->encode([$row], '7', '1'))->rows[0];

        $this->assertSame(['id', 'numeric', 'binary', 'nullable'], array_keys((array) $first));
        $this->assertSame(7, $first->id);
        $this->assertSame('007', $first->numeric);
        $this->assertSame("\x00\xff", $first->binary);
        $this->assertNull($first->nullable);
        $this->assertNotSame($first, $second);
    }

    public function test_the_serializer_only_decodes_marked_payloads(): void
    {
        $serializer = new CacheSerializer('php');

        $this->assertSame(['a' => 1], $serializer->decode($serializer->encode(['a' => 1])));
        $this->assertNull($serializer->decode(serialize(['a' => 1])));
        $this->assertNull($serializer->decode('not a payload at all'));
        $this->assertNull($serializer->decode(''));
    }

    public function test_raw_result_codec_validates_envelopes(): void
    {
        $codec = new RawResultCodec(new CacheSerializer);
        $row = (object) ['id' => 1, 'amount' => '01.20'];

        $encoded = $codec->encode([$row], '7', '1', ['dep' => '12'], '3');
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
            rootVersion: '1',
            ids: ['i:42', 'i:7', 'i:42'],
            versions: ['b' => '2', 'a' => '1'],
            tagVersion: '3',
            overlayRejected: true,
        );
        $decoded = $codec->decode($encoded);

        $this->assertTrue($decoded->valid);
        $this->assertSame(['i:42', 'i:7', 'i:42'], $decoded->ids);
        $this->assertSame(['a' => '1', 'b' => '2'], $decoded->versions);
        $this->assertSame('7', $decoded->epoch);
        $this->assertSame('4', $decoded->generation);
        $this->assertSame('3', $decoded->tagVersion);
        $this->assertTrue($decoded->overlayRejected);
        $this->assertFalse($codec->decode('{"f":3}')->valid);
    }

    public function test_membership_codec_rejects_invalid_dependency_versions(): void
    {
        $codec = new MembershipCodec;
        $integerVersion = json_encode([
            'f' => 5,
            'ep' => '0',
            'g' => '0',
            'ids' => 'i:1',
            'vec' => ['dependency' => 1],
            'rv' => '0',
        ], JSON_THROW_ON_ERROR);

        $this->assertFalse($codec->decode($integerVersion)->valid);
    }

    public function test_membership_codec_round_trips_an_empty_membership(): void
    {
        $codec = new MembershipCodec;

        $decoded = $codec->decode($codec->encode('7', '4', '1', []));

        $this->assertSame([], $decoded->ids);
        $this->assertFalse($decoded->overlayRejected);
    }

    public function test_membership_codec_rejects_the_previous_array_id_layout(): void
    {
        $codec = new MembershipCodec;
        $previous = json_encode([
            'f' => 5,
            'ep' => '7',
            'g' => '4',
            'ids' => ['i:42', 'i:7'],
            'vec' => [],
            'rv' => '0',
        ], JSON_THROW_ON_ERROR);

        $this->assertFalse($codec->decode($previous)->valid);
    }

    public function test_membership_round_trips_the_root_version(): void
    {
        $codec = new MembershipCodec;

        $encoded = $codec->encode(
            epoch: '7',
            generation: '2',
            ids: ['i:1', 'i:2'],
            versions: ['abc' => '3'],
            tagVersion: null,
            overlayRejected: false,
            rootVersion: '11',
        );

        $this->assertSame('11', $codec->decode($encoded)->rootVersion);
    }

    public function test_a_membership_without_a_root_version_is_rejected(): void
    {
        $legacy = json_encode([
            'f' => 5,
            'ep' => '1',
            'g' => '0',
            'ids' => 'i:1',
            'vec' => [],
        ], JSON_THROW_ON_ERROR);

        $this->assertFalse((new MembershipCodec)->decode($legacy)->valid);
    }

    public function test_result_payload_round_trips_the_root_version(): void
    {
        $codec = new RawResultCodec(new CacheSerializer('auto'));

        $encoded = $codec->encode(
            rows: [(object) ['id' => 1]],
            epoch: '7',
            versions: ['abc' => '3'],
            tagVersion: null,
            rootVersion: '11',
        );

        $this->assertSame('11', $codec->decode($encoded)->rootVersion);
    }

    public function test_a_result_payload_without_a_root_version_is_rejected(): void
    {
        $serializer = new CacheSerializer('auto');
        $codec = new RawResultCodec($serializer);
        $legacy = $serializer->encode([
            'f' => 5,
            'ep' => '7',
            'vec' => [],
            'rows' => [['id' => 1]],
        ]);

        $this->assertFalse($codec->decode($legacy)->valid);
    }
}
