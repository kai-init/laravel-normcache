<?php

namespace NormCache\Enums;

enum CacheReadOutcome: string
{
    case HIT = 'hit';

    case MISS = 'miss';

    case REPAIRED = 'repair';

    public function served(): bool
    {
        return $this !== self::MISS;
    }
}
