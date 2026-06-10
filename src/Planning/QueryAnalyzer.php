<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\Support\ProjectionClassifier;
use NormCache\Values\QueryInspection;

final class QueryAnalyzer
{
    public const SUBQUERY_WHERE_TYPES = [
        'Exists' => true,
        'NotExists' => true,
        'Sub' => true,
        'InSub' => true,
        'NotInSub' => true,
    ];

    public function inspect(
        QueryBuilder $base,
        string $table,
        ?array $resolvedColumns,
        array $primaryKeyIdentifiers = [],
        bool $includeTables = false,
    ): QueryInspection {
        return new QueryInspection(
            flags: $this->flags($base, $table, $resolvedColumns),
            primaryKeys: $primaryKeyIdentifiers === []
                ? null
                : self::extractPrimaryKeys($base, $primaryKeyIdentifiers),
            tables: $includeTables ? $this->extractTables($base, $table) : null,
        );
    }

    public function flags(
        QueryBuilder $base,
        string $table,
        ?array $resolvedColumns,
    ): int {
        $flags = 0;

        if ($base->from !== $table) {
            $flags |= QueryInspection::NON_CANONICAL_FROM;
        }

        if (!empty($base->joins)) {
            $flags |= QueryInspection::JOIN;
        }

        if (!empty($base->groups)) {
            $flags |= QueryInspection::GROUP;
        }

        if (!empty($base->havings)) {
            $flags |= QueryInspection::HAVING;
        }

        if (!empty($base->unions)) {
            $flags |= QueryInspection::UNION;
        }

        if (!empty($base->aggregate)) {
            $flags |= QueryInspection::AGGREGATE;
        }

        if (!empty($base->distinct)) {
            $flags |= QueryInspection::DISTINCT;
        }

        if ($base->lock !== null) {
            $flags |= QueryInspection::LOCK;
        }

        if (ProjectionClassifier::hasCalculatedColumns($resolvedColumns)) {
            $flags |= QueryInspection::CALCULATED_COLUMNS;
        }

        foreach ((array) $base->orders as $order) {
            if (($order['type'] ?? null) === 'Raw') {
                $flags |= QueryInspection::RAW_ORDER;
                break;
            }
        }

        $flags |= $this->inspectWheres((array) $base->wheres);

        return $flags;
    }

    public function extractTables(QueryBuilder $base, string $table): array
    {
        if (empty($base->joins)) {
            return [$table];
        }

        $tables = [$table];

        foreach ((array) $base->joins as $join) {
            if (is_string($join->table)) {
                $tables[] = preg_replace('/\s+as\s+\S+$/i', '', $join->table);
            }
        }

        return array_values(array_unique($tables));
    }

    public function directPrimaryKeys(
        QueryBuilder $base,
        string $table,
        ?array $resolvedColumns,
        array $primaryKeyIdentifiers,
    ): ?array {
        if ($base->from !== $table
            || !empty($base->joins)
            || !empty($base->groups)
            || !empty($base->havings)
            || !empty($base->unions)
            || !empty($base->aggregate)
            || !empty($base->distinct)
            || $base->lock !== null
            || ProjectionClassifier::hasCalculatedColumns($resolvedColumns)) {
            return null;
        }

        return self::extractPrimaryKeys($base, $primaryKeyIdentifiers);
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

    private function inspectWheres(array $wheres): int
    {
        $flags = 0;

        foreach ($wheres as $where) {
            $type = $where['type'] ?? '';

            if ($type === 'Raw' || $type === 'raw') {
                $flags |= QueryInspection::RAW_WHERE;
            }

            if (isset(self::SUBQUERY_WHERE_TYPES[$type])) {
                $flags |= QueryInspection::SUBQUERY_WHERE;
            } elseif (($type === 'In' || $type === 'NotIn') && $this->containsExpression((array) ($where['values'] ?? []))) {
                $flags |= QueryInspection::SUBQUERY_WHERE;
            } elseif ($type === 'Basic' && ($where['column'] ?? null) instanceof Expression) {
                $flags |= QueryInspection::SUBQUERY_WHERE;
            } elseif ($type === 'Nested' && isset($where['query'])) {
                $flags |= $this->inspectWheres((array) $where['query']->wheres);
            }

            if (($flags & (QueryInspection::RAW_WHERE | QueryInspection::SUBQUERY_WHERE))
                === (QueryInspection::RAW_WHERE | QueryInspection::SUBQUERY_WHERE)) {
                break;
            }
        }

        return $flags;
    }

    private function containsExpression(array $values): bool
    {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                return true;
            }
        }

        return false;
    }
}
