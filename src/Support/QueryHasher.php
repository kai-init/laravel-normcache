<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryHasher
{
    public static function hash(QueryBuilder $query): string
    {
        return hash('xxh3', $query->toSql() . json_encode($query->getBindings()));
    }
}
