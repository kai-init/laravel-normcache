<?php

namespace NormCache\Support;

use NormCache\Values\TableQueryReply;

final class RedisProtocol
{
    public const RESULT = 'result';

    public const MEMBERSHIP = 'membership';

    public const HIT = 'hit';

    public const MISS = 'miss';

    /** @param array<int, mixed> $reply */
    public static function decodeTableQuery(array $reply): TableQueryReply
    {
        $ownerToken = is_string($reply[5] ?? null) && $reply[5] !== ''
            ? $reply[5]
            : null;

        return new TableQueryReply(
            kind: self::status($reply) ?? self::MISS,
            version: (string) ($reply[1] ?? '0'),
            generation: is_string($reply[2] ?? null) && $reply[2] !== '' ? $reply[2] : null,
            payload: is_string($reply[3] ?? null) && $reply[3] !== '' ? $reply[3] : null,
            isOwner: ((int) ($reply[4] ?? 0)) === 1,
            ownerToken: $ownerToken,
        );
    }

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
}
