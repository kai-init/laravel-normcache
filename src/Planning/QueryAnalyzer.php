<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryAnalyzer
{
    public function forBuilder(
        QueryBuilder $base,
        string $table,
        ?array $resolvedColumns,
        array $primaryKeyIdentifiers = [],
        array $contextReasons = [],
    ): QueryAnalysis {
        return new QueryAnalysis(
            selectedColumns: $resolvedColumns,
            primaryKeys: self::extractPrimaryKeys($base, $primaryKeyIdentifiers),
            bypassReasons: BypassReasons::merge(
                $contextReasons,
                BypassReasons::forQuery($base, $table, $resolvedColumns),
            ),
        );
    }

    public static function extractPrimaryKeys(QueryBuilder $base, array $primaryKeyIdentifiers): ?array
    {
        if ($base->offset > 0) {
            return null;
        }

        if ($base->limit === 0) {
            return [];
        }

        if (count($base->wheres) !== 1) {
            return null;
        }

        $where = $base->wheres[0];
        $column = $where['column'] ?? null;

        if (!in_array($column, $primaryKeyIdentifiers, true)) {
            return null;
        }

        if (($where['type'] ?? null) === 'Basic' && ($where['operator'] ?? null) === '=') {
            return $where['value'] instanceof Expression ? null : [$where['value']];
        }

        if (!empty($base->orders) || $base->limit > 0) {
            return null;
        }

        if (($where['type'] ?? null) === 'In' || (($where['type'] ?? null) === 'InRaw' && isset($where['values']))) {
            $values = (array) $where['values'];

            foreach ($values as $value) {
                if ($value instanceof Expression) {
                    return null;
                }
            }

            sort($values);

            return $values;
        }

        return null;
    }
}
