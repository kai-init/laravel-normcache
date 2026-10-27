<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\RedisProtocol;
use NormCache\Tests\UnitTestCase;

final class RedisProtocolTest extends UnitTestCase
{
    public function test_decodes_result_hit(): void
    {
        $reply = RedisProtocol::decodeTableQuery(['result', '4', false, 'result-payload', 0, false]);

        $this->assertSame('result', $reply->kind);
        $this->assertSame('4', $reply->version);
        $this->assertNull($reply->generation);
        $this->assertSame('result-payload', $reply->payload);
        $this->assertFalse($reply->isOwner);
        $this->assertNull($reply->ownerToken);
    }

    public function test_decodes_membership_hit(): void
    {
        $reply = RedisProtocol::decodeTableQuery(['membership', '4', '7', 'membership-payload', 0, false]);

        $this->assertSame('membership', $reply->kind);
        $this->assertSame('4', $reply->version);
        $this->assertSame('7', $reply->generation);
        $this->assertSame('membership-payload', $reply->payload);
        $this->assertFalse($reply->isOwner);
        $this->assertNull($reply->ownerToken);
    }

    public function test_decodes_unclaimed_miss(): void
    {
        $reply = RedisProtocol::decodeTableQuery(['miss', '4', '7', false, 0, false]);

        $this->assertSame('miss', $reply->kind);
        $this->assertSame('4', $reply->version);
        $this->assertSame('7', $reply->generation);
        $this->assertNull($reply->payload);
        $this->assertFalse($reply->isOwner);
        $this->assertNull($reply->ownerToken);
    }

    public function test_decodes_owned_miss(): void
    {
        $reply = RedisProtocol::decodeTableQuery(['miss', '4', false, false, 1, 'claim-token']);

        $this->assertSame('miss', $reply->kind);
        $this->assertNull($reply->generation);
        $this->assertNull($reply->payload);
        $this->assertTrue($reply->isOwner);
        $this->assertSame('claim-token', $reply->ownerToken);
    }

    public function test_decodes_waiting_miss(): void
    {
        $reply = RedisProtocol::decodeTableQuery(['miss', '4', '7', false, 0, 'other-token']);

        $this->assertFalse($reply->isOwner);
        $this->assertSame('other-token', $reply->ownerToken);
    }

    public function test_false_and_null_placeholders_decode_as_null(): void
    {
        foreach ([false, null] as $placeholder) {
            $reply = RedisProtocol::decodeTableQuery(['result', '4', $placeholder, $placeholder, 0, $placeholder]);

            $this->assertNull($reply->generation);
            $this->assertNull($reply->payload);
            $this->assertNull($reply->ownerToken);
        }
    }

    public function test_missing_or_invalid_values_use_defaults(): void
    {
        $reply = RedisProtocol::decodeTableQuery([false, 4, '', '', '1', '']);

        $this->assertSame('miss', $reply->kind);
        $this->assertSame('4', $reply->version);
        $this->assertNull($reply->generation);
        $this->assertNull($reply->payload);
        $this->assertTrue($reply->isOwner);
        $this->assertNull($reply->ownerToken);

        $empty = RedisProtocol::decodeTableQuery([]);
        $this->assertSame('miss', $empty->kind);
        $this->assertSame('0', $empty->version);
        $this->assertNull($empty->generation);
        $this->assertNull($empty->payload);
        $this->assertFalse($empty->isOwner);
        $this->assertNull($empty->ownerToken);
    }
}
