<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\RedisProtocol;
use NormCache\Tests\UnitTestCase;

final class RedisProtocolTest extends UnitTestCase
{
    public function test_extracts_named_values_from_positional_replies(): void
    {
        $canonical = [RedisProtocol::HIT, '4', '7', 'canonical-payload'];
        $result = [RedisProtocol::RESULT, '4', 'result-payload'];

        $this->assertSame(RedisProtocol::HIT, RedisProtocol::status($canonical));
        $this->assertSame('4', RedisProtocol::version($canonical));
        $this->assertSame('7', RedisProtocol::version($canonical, 2));
        $this->assertSame(
            'canonical-payload',
            RedisProtocol::canonicalPayload($canonical),
        );
        $this->assertSame('result-payload', RedisProtocol::resultPayload($result));
    }

    public function test_invalid_or_missing_reply_values_use_safe_defaults(): void
    {
        $reply = [false, 4];

        $this->assertNull(RedisProtocol::status($reply));
        $this->assertSame('0', RedisProtocol::version($reply));
        $this->assertNull(RedisProtocol::value($reply, 9));
    }
}
