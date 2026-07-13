<?php

namespace NormCache\Enums;

enum CacheOperation
{
    case Models;
    case Scalar;
    case PaginationCount;
    case Pivot;
    case Through;
}
