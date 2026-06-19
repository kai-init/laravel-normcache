<?php

namespace NormCache\Enums;

enum ResultKind: string
{
    case Collection = 'collection';
    case Value = 'value';
    case Count = 'count';
    case Pluck = 'pluck';
    case PaginationCount = 'pagination_count';
    case Sum = 'sum';
    case Avg = 'avg';
    case Min = 'min';
    case Max = 'max';
    case Exists = 'exists';
}
