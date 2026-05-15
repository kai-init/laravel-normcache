<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryHasher
{
    public static function fromQuery(QueryBuilder $query): string
    {
        return self::hash($query->toSql() . json_encode([
            'bindings' => $query->getBindings(),
            'useWritePdo' => $query->useWritePdo,
        ]));
    }

    public static function hash(string $data): string
    {
        return hash('xxh3', $data);
    }
}
