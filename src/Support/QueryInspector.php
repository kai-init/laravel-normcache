<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Database\Query\Expression;

final class QueryInspector
{
    private const COLUMN_IDENTIFIER = '[`"]?[A-Za-z_][A-Za-z0-9_]*[`"]?';

    public static function isPureModelQuery(QueryBuilder $base, string $table): bool
    {
        foreach ((array) $base->orders as $order) {
            if (isset($order['type']) && $order['type'] === 'Raw') {
                return false;
            }
        }

        if (self::hasSubqueryWheres((array) $base->wheres)) {
            return false;
        }

        if (!self::isCanonicalFrom($base, $table)) {
            return false;
        }

        return empty($base->joins)
            && empty($base->groups)
            && empty($base->havings)
            && empty($base->unions)
            && empty($base->aggregate)
            && empty($base->distinct)
            && is_null($base->lock);
    }

    public static function resolveSelectedColumns(QueryBuilder $base, ?array $fallback): ?array
    {
        $columns = $base->columns ?? (($fallback === null || $fallback === ['*']) ? null : $fallback);

        if ($columns === null || $columns === ['*']) {
            return null;
        }

        foreach ($columns as $column) {
            if ($column instanceof Expression || !str_ends_with((string) $column, '*')) {
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
            if ($column instanceof Expression) {
                return true;
            }

            if (!self::isCacheableSelectedColumn((string) $column)) {
                return true;
            }
        }

        return false;
    }

    public static function extractPrimaryKeys(QueryBuilder $base, string $pk, string $qualifiedPk): ?array
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

        if (!in_array($column, [$pk, $qualifiedPk], true)) {
            return null;
        }

        if ($where['type'] === 'Basic' && $where['operator'] === '=') {
            return [$where['value']];
        }

        if (!empty($base->orders) || $base->limit > 0) {
            return null;
        }

        if ($where['type'] === 'In' || $where['type'] === 'InRaw') {
            $values = $where['values'];
            sort($values);

            return $values;
        }

        return null;
    }

    /** @return array<string, string> output => source */
    public static function normalizeProjection(array $columns): array
    {
        $normalized = [];

        foreach ($columns as $column) {
            [$source, $output] = self::parseProjection((string) $column);
            $normalized[$output] = $source;
        }

        return $normalized;
    }

    public static function projectAttributes(array $attributes, array $projection): array
    {
        $projected = [];

        foreach ($projection as $output => $source) {
            if (array_key_exists($source, $attributes)) {
                $projected[$output] = $attributes[$source];
            }
        }

        return $projected;
    }

    private static function isCanonicalFrom(QueryBuilder $base, string $table): bool
    {
        $from = $base->from;

        if (!is_string($from)) {
            return false;
        }

        return $from === $table
            || (bool) preg_match('/^' . preg_quote($table, '/') . '\s+as\s+\w+$/i', $from);
    }

    private static function hasSubqueryWheres(array $wheres): bool
    {
        static $subqueryTypes = ['Exists', 'NotExists', 'Sub', 'InSub', 'NotInSub'];

        foreach ($wheres as $where) {
            $type = $where['type'] ?? '';

            if (in_array($type, $subqueryTypes, true)) {
                return true;
            }

            if ($type === 'Nested' && isset($where['query'])) {
                if (self::hasSubqueryWheres((array) $where['query']->wheres)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static function isCacheableSelectedColumn(string $column): bool
    {
        $column = trim($column);

        if (self::isWildcardColumn($column)) {
            return false;
        }

        if (stripos($column, ' as ') !== false) {
            return self::isCacheableAliasedColumn($column);
        }

        return self::isColumnIdentifier($column);
    }

    private static function isCacheableAliasedColumn(string $column): bool
    {
        $segments = preg_split('/\s+as\s+/i', trim($column));

        return count($segments) === 2
            && self::isColumnIdentifier($segments[0])
            && self::isColumnIdentifier($segments[1], false);
    }

    private static function isColumnIdentifier(string $column, bool $allowQualifier = true): bool
    {
        $pattern = $allowQualifier
            ? '/^' . self::COLUMN_IDENTIFIER . '(?:\\.' . self::COLUMN_IDENTIFIER . ')?$/'
            : '/^' . self::COLUMN_IDENTIFIER . '$/';

        return (bool) preg_match($pattern, trim($column));
    }

    private static function isWildcardColumn(string $column): bool
    {
        return $column === '*' || str_ends_with($column, '.*');
    }

    private static function parseProjection(string $column): array
    {
        $column = trim($column);
        $segments = preg_split('/\s+as\s+/i', $column);

        if (count($segments) === 2) {
            return [self::unqualify($segments[0]), self::unqualify($segments[1])];
        }

        $name = self::unqualify($column);

        return [$name, $name];
    }

    private static function unqualify(string $column): string
    {
        $column = trim($column);
        $dotPos = strrpos($column, '.');

        if ($dotPos !== false) {
            $column = substr($column, $dotPos + 1);
        }

        return trim($column, " \t\n\r\0\x0B`\"[]");
    }
}
