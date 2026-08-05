<?php

namespace NormCache\Support;

final class RedisProtocol
{
    public const RESULT = 'result';

    public const MEMBERSHIP = 'membership';

    public const HIT = 'hit';

    public const CORRUPT = 'corrupt';

    /** @param array<int, mixed> $reply */
    public static function status(array $reply): ?string
    {
        $status = $reply[0] ?? null;

        return is_string($status) ? $status : null;
    }

    /** @param array<int, mixed> $reply */
    public static function version(array $reply, int $index = 1): string
    {
        $version = $reply[$index] ?? null;

        return is_string($version) ? $version : '0';
    }

    /** @param array<int, mixed> $reply */
    public static function value(array $reply, int $index): mixed
    {
        return $reply[$index] ?? null;
    }

    /** @param array<int, mixed> $reply */
    public static function resultPayload(array $reply): mixed
    {
        return self::value($reply, 2);
    }

    /** @param array<int, mixed> $reply */
    public static function canonicalPayload(array $reply): mixed
    {
        return self::value($reply, 3);
    }
}
