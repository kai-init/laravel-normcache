<?php

namespace NormCache\Enums;

enum ResultKind: string
{
    case Collection = 'collection';
    case Value = 'value';
    case Count = 'count';
    case Pluck = 'pluck';
    case PaginationCount = 'pagination_count';
    case Aggregate = 'aggregate';
}
