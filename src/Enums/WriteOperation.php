<?php

namespace NormCache\Enums;

enum WriteOperation: string
{
    case Insert = 'insert';
    case Update = 'update';
    case Delete = 'delete';
    case Truncate = 'truncate';
    case Increment = 'increment';

    public function isInsert(): bool
    {
        return $this === self::Insert;
    }
}
