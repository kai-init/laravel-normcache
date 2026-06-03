<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;

final class BypassReasons
{
    /**
     * Categories:
     *   dependency    — dependency tracking can't safely cover this query
     *   normalization — result can't be decomposed into model cache keys
     *   safety        — bypassed for query correctness; no caching workaround
     *
     * @param  array<int,mixed>|null  $resolvedColumns  null skips the calculated-column check
     * @return array<string, list<string>>
     */
    public static function forQuery(QueryBuilder $base, string $table, ?array $resolvedColumns = null): array
    {
        $dependency = [];
        $normalization = [];
        $safety = [];

        if (self::hasRawOrderBypass($base)) {
            $dependency[] = 'raw ORDER expression';
        }

        if (self::hasRawWhereBypass((array) $base->wheres)) {
            $dependency[] = 'raw WHERE expression';
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

        if (QueryAnalyzer::hasCalculatedColumns($resolvedColumns)) {
            $normalization[] = 'calculated or raw SELECT expressions';
        }

        return array_filter([
            'dependency' => $dependency,
            'normalization' => $normalization,
            'safety' => $safety,
        ]);
    }

    public static function labels(): array
    {
        return [
            'dependency' => "can't infer cache dependency",
            'normalization' => "result can't be normalized into model keys",
            'safety' => 'bypassed for query correctness',
            'opted_out' => 'explicitly disabled',
        ];
    }

    public static function merge(array ...$groups): array
    {
        $merged = [];

        foreach ($groups as $group) {
            foreach ($group as $category => $reasons) {
                $merged[$category] = array_values(array_unique([
                    ...($merged[$category] ?? []),
                    ...$reasons,
                ]));
            }
        }

        return array_filter($merged);
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
}
