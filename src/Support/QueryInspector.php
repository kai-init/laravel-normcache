<?php

namespace NormCache\Support;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryInspector
{
    private const COLUMN_IDENTIFIER = '[`"]?[A-Za-z_][A-Za-z0-9_]*[`"]?';

    public static function isCacheable(QueryBuilder $base, string $table, ?array $resolvedColumns = null): bool
    {
        return self::isStructurallyCacheable($base, $table, $resolvedColumns)
            && !self::hasDependencyBypass($base);
    }

    public static function isStructurallyCacheable(QueryBuilder $base, string $table, ?array $resolvedColumns = null): bool
    {
        return !self::hasNormalizationBypass($base, $table, $resolvedColumns)
            && !self::hasSafetyBypass($base);
    }

    public static function hasDependencyBypass(QueryBuilder $base): bool
    {
        return self::hasRawOrderBypass($base)
            || self::hasSubqueryWheres((array) $base->wheres);
    }

    public static function categoryLabels(): array
    {
        return [
            'dependency' => "can't infer cache dependency",
            'normalization' => "result can't be normalized into model keys",
            'safety' => 'bypassed for query correctness',
            'opted_out' => 'explicitly disabled',
        ];
    }

    /**
     * Categories:
     *   dependency    — dependency tracking can't safely cover this query
     *   normalization — result can't be decomposed into model cache keys
     *   safety        — bypassed for query correctness; no caching workaround
     *
     * @param  array<int,mixed>|null  $resolvedColumns  null skips the calculated-column check
     * @return array<string, list<string>>
     */
    public static function bypassReasons(QueryBuilder $base, string $table, ?array $resolvedColumns = null): array
    {
        $dependency = [];
        $normalization = [];
        $safety = [];

        if (self::hasRawOrderBypass($base)) {
            $dependency[] = 'raw ORDER expression';
        }

        if (self::hasSubqueryWheres((array) $base->wheres)) {
            $dependency[] = 'subquery WHERE (whereHas/whereExists)';
        }

        if (!self::isCanonicalFrom($base, $table)) {
            $normalization[] = 'non-standard FROM (subquery or raw expression)';
        }

        if (!empty($base->joins)) {
            $normalization[] = 'JOIN clauses';
        }

        if (!empty($base->groups)) {
            $normalization[] = 'GROUP BY';
        }

        if (!empty($base->havings)) {
            $normalization[] = 'HAVING';
        }

        if (!empty($base->unions)) {
            $normalization[] = 'UNION';
        }

        if (!empty($base->aggregate)) {
            $normalization[] = 'aggregate function (count/sum/etc.)';
        }

        if (!empty($base->distinct)) {
            $normalization[] = 'DISTINCT';
        }

        if (!is_null($base->lock)) {
            $safety[] = 'query lock (SELECT FOR UPDATE)';
        }

        if (self::hasCalculatedColumns($resolvedColumns)) {
            $normalization[] = 'calculated or raw SELECT expressions';
        }

        return array_filter([
            'dependency' => $dependency,
            'normalization' => $normalization,
            'safety' => $safety,
        ]);
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
            if (!is_string($column)) {
                return true;
            }

            if (!self::isCacheableSelectedColumn($column)) {
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
            if ($where['value'] instanceof Expression) {
                return null;
            }

            // Single-ID lookups are unaffected by ORDER BY or LIMIT.
            return [$where['value']];
        }

        // Multi-ID lookups need SQL to apply ORDER BY or LIMIT before model-cache fetches.
        if (!empty($base->orders) || $base->limit > 0) {
            return null;
        }

        if ($where['type'] === 'In' || $where['type'] === 'InRaw') {
            $values = $where['values'];

            if (self::containsExpression((array) $values)) {
                return null;
            }

            sort($values);

            return $values;
        }

        return null;
    }

    public static function isCacheableScalarColumn(mixed $column): bool
    {
        return is_string($column)
            && (bool) preg_match('/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/', $column);
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

    private static function hasNormalizationBypass(QueryBuilder $base, string $table, ?array $resolvedColumns): bool
    {
        return !self::isCanonicalFrom($base, $table)
            || !empty($base->joins)
            || !empty($base->groups)
            || !empty($base->havings)
            || !empty($base->unions)
            || !empty($base->aggregate)
            || !empty($base->distinct)
            || self::hasCalculatedColumns($resolvedColumns);
    }

    public static function hasSafetyBypass(QueryBuilder $base): bool
    {
        return !is_null($base->lock);
    }

    private static function hasRawWhereBypass(array $wheres): bool
    {
        foreach ($wheres as $where) {
            $type = strtolower((string) ($where['type'] ?? ''));

            if ($type === 'raw') {
                return true;
            }

            if ($type === 'nested' && isset($where['query'])) {
                if (self::hasRawWhereBypass((array) $where['query']->wheres)) {
                    return true;
                }
            }
        }

        return false;
    }

    private static function hasRawOrderBypass(QueryBuilder $base): bool
    {
        foreach ((array) $base->orders as $order) {
            if (isset($order['type']) && $order['type'] === 'Raw') {
                return true;
            }
        }

        return false;
    }

    private static function hasSubqueryWheres(array $wheres): bool
    {
        static $subqueryTypes = ['Exists', 'NotExists', 'Sub', 'InSub', 'NotInSub'];

        foreach ($wheres as $where) {
            $type = $where['type'] ?? '';

            if (in_array($type, $subqueryTypes, true)) {
                return true;
            }

            if (in_array($type, ['In', 'NotIn'], true) && self::containsExpression((array) ($where['values'] ?? []))) {
                return true;
            }

            if ($type === 'Basic' && ($where['column'] ?? null) instanceof Expression) {
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

    private static function containsExpression(array $values): bool
    {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                return true;
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
