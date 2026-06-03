<?php

namespace NormCache\Enums;

enum CacheOperation
{
    case Models;
    case Scalar;
    case PaginationCount;
    case RelationAggregate;
    case BelongsToEagerLoad;
    case MorphToEagerLoad;
    case Pivot;
    case Through;
}
