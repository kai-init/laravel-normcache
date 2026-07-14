<?php

namespace NormCache\Planning;

use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\Values\QueryInspection;

final class BypassReasons
{
    /**
     * Categories:
     *   dependency    — dependency tracking can't safely cover this query
     *   normalization — result can't be decomposed into model cache keys
     *   safety        — bypassed for query correctness; no caching workaround
     *   space         — cache-space membership or registration prevents caching
     *
     * @param  array<int,mixed>|null  $resolvedColumns  null skips the calculated-column check
     * @return array<string, list<string>>
     */
    public static function forQuery(QueryBuilder $base, string $table, ?array $resolvedColumns = null): array
    {
        static $analyzer;

        return self::fromInspection(
            ($analyzer ??= new QueryAnalyzer)->inspect($base, $table, $resolvedColumns),
        );
    }

    /** @return array<string, list<string>> */
    public static function fromInspection(QueryInspection $inspection): array
    {
        $dependency = [];
        $normalization = [];
        $safety = [];

        if ($inspection->has(QueryInspection::RAW_ORDER)) {
            $dependency[] = 'raw ORDER expression';
        }

        if ($inspection->has(QueryInspection::RAW_WHERE)) {
            $dependency[] = 'raw WHERE expression';
        }

        if ($inspection->has(QueryInspection::SUBQUERY_WHERE | QueryInspection::EXISTS_WHERE)) {
            $dependency[] = 'subquery WHERE (whereHas/whereExists)';
        }

        if ($inspection->has(QueryInspection::NON_CANONICAL_FROM)) {
            $normalization[] = 'non-standard FROM (subquery or raw expression)';
        }

        if ($inspection->has(QueryInspection::JOIN)) {
            $normalization[] = 'JOIN clauses';
        }

        if ($inspection->has(QueryInspection::GROUP)) {
            $normalization[] = 'GROUP BY';
        }

        if ($inspection->has(QueryInspection::HAVING)) {
            $normalization[] = 'HAVING';
        }

        if ($inspection->has(QueryInspection::UNION)) {
            $normalization[] = 'UNION';
        }

        if ($inspection->has(QueryInspection::AGGREGATE)) {
            $normalization[] = 'aggregate function (count/sum/etc.)';
        }

        if ($inspection->has(QueryInspection::DISTINCT)) {
            $normalization[] = 'DISTINCT';
        }

        if ($inspection->has(QueryInspection::LOCK)) {
            $safety[] = 'query lock (SELECT FOR UPDATE)';
        }

        if ($inspection->has(QueryInspection::CALCULATED_COLUMNS)) {
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
            'space' => 'cross-space dependencies',
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
}
