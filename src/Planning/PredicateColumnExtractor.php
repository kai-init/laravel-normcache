<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder;
use NormCache\Database\QueryBuilder;
use NormCache\Support\ColumnName;

final class PredicateColumnExtractor
{
    private const UNDERSTOOD_WHERE_TYPES = [
        'Basic', 'In', 'InRaw', 'NotIn', 'NotInRaw', 'Null', 'NotNull', 'between', 'Nested',
    ];

    /** @return list<string>|null */
    public function extract(QueryBuilder $query): ?array
    {
        $columns = [];

        if (!$this->collectWheres($query->wheres, $columns)) {
            return null;
        }

        foreach ($query->orders ?? [] as $order) {
            $column = $order['column'] ?? null;

            if (!is_string($column) || str_contains($column, '->')) {
                return null;
            }

            $columns[ColumnName::unqualified($column)] = true;
        }

        $names = array_map(strval(...), array_keys($columns));
        sort($names, SORT_STRING);

        return $names;
    }

    /** @param array<string, true> $columns */
    private function collectWheres(array $wheres, array &$columns): bool
    {
        foreach ($wheres as $where) {
            $type = $where['type'] ?? null;

            if (!in_array($type, self::UNDERSTOOD_WHERE_TYPES, true)) {
                return false;
            }

            if ($type === 'Nested') {
                $nested = $where['query'] ?? null;

                if (!$nested instanceof Builder || !$this->collectWheres($nested->wheres, $columns)) {
                    return false;
                }

                continue;
            }

            if (in_array($type, ['In', 'InRaw', 'NotIn', 'NotInRaw', 'between'], true)
                && !$this->isLiteralValueList($where['values'] ?? null)) {
                return false;
            }

            if ($type === 'Basic' && ($where['value'] ?? null) instanceof Expression) {
                return false;
            }

            $column = $where['column'] ?? null;

            if (!is_string($column) || str_contains($column, '->')) {
                return false;
            }

            $columns[ColumnName::unqualified($column)] = true;
        }

        return true;
    }

    /** Subquery and between expressions are stored inline in `values`. */
    private function isLiteralValueList(mixed $values): bool
    {
        if (!is_iterable($values)) {
            return false;
        }

        foreach ($values as $value) {
            if ($value instanceof Expression) {
                return false;
            }
        }

        return true;
    }
}
