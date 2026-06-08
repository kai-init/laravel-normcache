<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Query\Builder as QueryBuilder;

final class ProjectionClassifier
{
    public static function resolve(QueryBuilder $base, ?array $fallback): ?array
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

    public static function isExactFullModelProjection(?array $columns, string $table): bool
    {
        if ($columns === null || $columns === ['*']) {
            return true;
        }

        foreach ($columns as $column) {
            if (!is_string($column)) {
                return false;
            }

            if ($column === '*' || $column === "{$table}.*") {
                continue;
            }

            // Internal artifacts (laravel_through_key or pivot_*) are not pollutants
            if (str_contains($column, ' as laravel_through_key') || str_contains($column, ' as pivot_')) {
                continue;
            }

            return false;
        }

        return true;
    }

    public static function containsWildcard(?array $columns): bool
    {
        if ($columns === null) {
            return false;
        }

        foreach ($columns as $column) {
            if (is_string($column) && str_ends_with($column, '*')) {
                return true;
            }
        }

        return false;
    }

    public static function hasRequiredKey(array $columns, string $table, string $key): bool
    {
        $qualified = "{$table}.{$key}";

        return in_array('*', $columns, true)
            || in_array("{$table}.*", $columns, true)
            || in_array($key, $columns, true)
            || in_array($qualified, $columns, true);
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

    public static function classifyForRelation(Builder $query, array $columns, string $relatedTable, string $relatedKey): array
    {
        $queryColumns = $query->toBase()->columns;
        $resolved = $queryColumns ?? ($columns === ['*'] ? null : $columns);

        $shouldCacheRelatedModels = false;
        $selectedRelatedColumns = null;

        if ($resolved === null) {
            $shouldCacheRelatedModels = true;
            $selectedRelatedColumns = null;
        } else {
            $shouldCacheRelatedModels = self::isExactFullModelProjection($resolved, $relatedTable);
            $selectedRelatedColumns = $shouldCacheRelatedModels ? null : $resolved;
        }

        return [
            'shouldCacheRelatedModels' => $shouldCacheRelatedModels,
            'selectedRelatedColumns' => $selectedRelatedColumns,
            'relatedKeyInProjection' => self::hasRequiredKey($resolved ?? ['*'], $relatedTable, $relatedKey),
            'resolvedColumns' => $resolved,
        ];
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
        $columnIdentifier = '[`"]?[A-Za-z_][A-Za-z0-9_]*[`"]?';
        $pattern = $allowQualifier
            ? '/^' . $columnIdentifier . '(?:\\.' . $columnIdentifier . ')?$/'
            : '/^' . $columnIdentifier . '$/';

        return (bool) preg_match($pattern, trim($column));
    }
}
