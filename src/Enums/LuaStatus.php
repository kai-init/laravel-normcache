<?php

namespace NormCache\Enums;

enum LuaStatus: string
{
    case Hit = 'hit';
    case Miss = 'miss';
    case Building = 'building';
    case Empty = 'empty';
    case Corrupt = 'corrupt';

    // Degenerate/unrecognised input defaults to Miss: rebuild from DB.
    public static function fromLua(mixed $status): self
    {
        return self::tryFrom((string) $status) ?? self::Miss;
    }

    // This status is expected to include a payload slot.
    public function hasPayload(): bool
    {
        return $this === self::Hit;
    }
}
