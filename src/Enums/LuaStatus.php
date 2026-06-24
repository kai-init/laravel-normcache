<?php

namespace NormCache\Enums;

enum LuaStatus: string
{
    case Hit = 'hit';
    case Miss = 'miss';
    case Stale = 'stale';
    case Building = 'building';
    case Empty = 'empty';
    case Corrupt = 'corrupt';

    /** Degenerate/unrecognised input defaults to Miss: rebuild from DB */
    public static function fromLua(mixed $status): self
    {
        return self::tryFrom((string) $status) ?? self::Miss;
    }

    /** This status's payload carries servable data (ids + models). */
    public function servesData(): bool
    {
        return $this === self::Hit || $this === self::Stale;
    }
}
