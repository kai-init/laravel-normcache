<?php

namespace NormCache\Tests\Unit;

use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;

final class PrimaryKeyMetadataTest extends UnitTestCase
{
    public function test_integer_tokens_are_canonical_decimal_values(): void
    {
        $metadata = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);

        $this->assertSame('i:42', $metadata->token(42));
        $this->assertSame('i:-42', $metadata->token('-42'));
        $this->assertNull($metadata->token('0042'));
        $this->assertNull($metadata->token(4.2));
        $this->assertNull($metadata->token('4e2'));
        $this->assertSame(
            'i:9223372036854775807',
            $metadata->token('9223372036854775807'),
        );
        $this->assertSame(
            9223372036854775807,
            $metadata->valueFromToken('i:9223372036854775807'),
        );
        $this->assertSame(
            'i:-9223372036854775808',
            $metadata->token('-9223372036854775808'),
        );

        // Beyond native 64-bit range: never a legitimate PK value, must be rejected
        // rather than silently truncated.
        $this->assertNull($metadata->token('18446744073709551615'));
        $this->assertNull($metadata->valueFromToken('i:18446744073709551615'));
    }

    public function test_string_tokens_preserve_exact_bytes_without_key_delimiters(): void
    {
        $metadata = new PrimaryKeyMetadata('uuid', PrimaryKeyMetadata::STRING);

        $this->assertSame('s:NDI', $metadata->token('42'));
        $this->assertSame('s:AP9hOnt9', $metadata->token("\x00\xffa:{}"));
        $this->assertSame('42', $metadata->valueFromToken('s:NDI'));
        $this->assertSame("\x00\xffa:{}", $metadata->valueFromToken('s:AP9hOnt9'));
    }

    public function test_tokens_must_match_the_canonical_value_representation(): void
    {
        $integer = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
        $string = new PrimaryKeyMetadata('uuid', PrimaryKeyMetadata::STRING);

        $this->assertTrue($integer->matchesToken(42, 'i:42'));
        $this->assertTrue($integer->matchesToken('42', 'i:42'));
        $this->assertFalse($integer->matchesToken('42', 's:42'));
        $this->assertFalse($integer->matchesToken('42', 'i:0042'));
        $this->assertTrue($string->matchesToken('42', 's:NDI'));
        $this->assertFalse($string->matchesToken('42', 'i:42'));
    }
}
