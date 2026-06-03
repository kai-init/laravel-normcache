<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryAnalyzer
{
    private const COLUMN_IDENTIFIER = '[`"]?[A-Za-z_][A-Za-z0-9_]*[`"]?';

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

    public static function resolveSelectedColumns(QueryBuilder $base, ?array $fallback): ?array
    {
        $columns = $base->columns ?? (($fallback === null || $fallback === ['*']) ? null : $fallback);

        if ($columns === null || $columns === ['*']) {
            return null;
        }

        foreach ($columns as $column) {
            if (!is_string($column) || !str_ends_with($column, '*')) {
                return $columns;
            }
        }

        return null;
    }

    public static function hasCalculatedColumns(?array $columns): bool
    {
        if ($columns === null) {
            return false;
        }

        foreach ($columns as $column) {
            if (!is_string($column) || !self::isCacheableSelectedColumn($column)) {
                return true;
            }
        }

        return false;
    }

    private static function isCacheableSelectedColumn(string $column): bool
    {
        $column = trim($column);

        if ($column === '*' || str_ends_with($column, '.*')) {
            return true;
        }

        if (stripos($column, ' as ') !== false) {
            $segments = preg_split('/\s+as\s+/i', trim($column));

            return count($segments) === 2
                && self::isColumnIdentifier($segments[0])
                && self::isColumnIdentifier($segments[1], false);
        }

        return self::isColumnIdentifier($column);
    }

    private static function isColumnIdentifier(string $column, bool $allowQualifier = true): bool
    {
        $pattern = $allowQualifier
            ? '/^' . self::COLUMN_IDENTIFIER . '(?:\\.' . self::COLUMN_IDENTIFIER . ')?$/'
            : '/^' . self::COLUMN_IDENTIFIER . '$/';

        return (bool) preg_match($pattern, trim($column));
    }
}
