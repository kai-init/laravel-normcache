<?php

namespace NormCache\Enums;

enum CacheStatus: string
{
    case Hit = 'hit';
    case Miss = 'miss';
    case Building = 'building';
    case Empty = 'empty';
}
