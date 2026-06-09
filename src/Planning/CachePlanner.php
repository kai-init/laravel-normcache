<?php

namespace NormCache\Planning;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Facades\Log;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Enums\CacheOperation;
use NormCache\Facades\NormCache;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\QueryAnalysis;

final class CachePlanner
{
    public function __construct(
        private readonly QueryAnalyzer $analyzer = new QueryAnalyzer,
    ) {}

    public function plan(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlanContext $context,
    ): CachePlan {
        $slotting = NormCache::isSlotting();
        $contextReasons = $this->resolveContextReasons($builder, $context);

        $analysis = $this->analyzer->forBuilder(
            $base,
            $builder->getModel()->getTable(),
            $context->columns,
            [$builder->getModel()->getKeyName(), $builder->getModel()->getQualifiedKeyName()],
            $contextReasons,
        );

        // 1. Resolve Dependencies
        $modelClass = $builder->getModel()::class;
        $explicit = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $inferred = $context->inferredDependencies;
        $hasExplicit = $explicit !== null || $explicitTables !== [];

        $dependencies = $hasExplicit
            ? new DependencySet(
                models: array_keys(array_flip([$modelClass, ...$inferred->models, ...($explicit ?? [])])),
                tables: array_values(array_unique([...$inferred->tables, ...$explicitTables])),
            )
            : ($analysis->hasDependencyBypass() || !$inferred->safe
                ? DependencySet::unsafe(array_values(array_unique([
                    ...$analysis->dependencyReasons(),
                    ...$inferred->reasons,
                    ...($analysis->hasDependencyBypass() ? ['Query requires explicit dependencies.'] : []),
                ])))
                : new DependencySet(
                    models: array_keys(array_flip([$modelClass, ...$inferred->models])),
                    tables: $inferred->tables,
                ));

        // 2. Qualify Normalization
        $normalizable = false;
        $normalizationReasons = [];
        if ($context->requiresNormalization()) {
            if ($analysis->hasSafetyBypass()) {
                $normalizationReasons[] = 'Query has safety bypass.';
            } elseif ($analysis->hasDependencyBypass()) {
                $normalizationReasons[] = 'Query has dependency bypass.';
            } elseif ($analysis->hasNormalizationBypass()) {
                $normalizationReasons[] = 'Query shape cannot be normalized.';
            } else {
                $normalizable = true;
            }
        }

        // 3. Resolve Cache Mode
        $hasResultDependencies = $hasExplicit || $inferred->models !== [] || $inferred->tables !== [];
        $isResultStyleOperation = in_array($context->operation, [
            CacheOperation::Scalar,
            CacheOperation::PaginationCount,
            CacheOperation::Pivot,
            CacheOperation::Through,
        ], true);

        if ($analysis->hasOptedOutBypass()) {
            return new CachePlan(
                mode: CacheMode::Bypass,
                operation: $context->operation,
                dependencies: $dependencies,
                normalizable: false,
                columns: $context->columns,
                primaryKeys: $analysis->primaryKeys,
                reasons: $analysis->optedOutReasons(),
                bypassReasons: ['opted_out' => $analysis->optedOutReasons()],
            );
        }

        $bypassReasons = $analysis->bypassReasons;

        // Relation-specific bypass relaxations or strictness
        if ($context->operation === CacheOperation::BelongsToEagerLoad || $context->operation === CacheOperation::MorphToEagerLoad) {
            if ($analysis->primaryKeys === null) {
                $bypassReasons['normalization'][] = 'eager load requires primary key lookup';
            }
        }

        if ($context->operation === CacheOperation::Pivot || $context->operation === CacheOperation::Through) {
            // Pivot/Through operations allow exactly one join (to the intermediate table)
            if (count($bypassReasons['normalization'] ?? []) === 1 && $bypassReasons['normalization'][0] === 'JOIN clauses') {
                if (count($base->joins ?? []) === 1) {
                    unset($bypassReasons['normalization']);
                }
            }
        }

        if (isset($bypassReasons['safety'])) {
            return new CachePlan(
                mode: CacheMode::Bypass,
                operation: $context->operation,
                dependencies: $dependencies,
                normalizable: false,
                columns: $context->columns,
                primaryKeys: $analysis->primaryKeys,
                reasons: $bypassReasons['safety'],
                bypassReasons: ['safety' => $bypassReasons['safety']],
            );
        }

        if ($normalizable && $dependencies->safe && empty($bypassReasons['normalization'])) {
            $isMultiDependency = count($dependencies->models) + count($dependencies->tables) > 1;

            if (!$slotting || !$isMultiDependency) {
                return new CachePlan(
                    mode: CacheMode::Normalized,
                    operation: $context->operation,
                    dependencies: $dependencies,
                    normalizable: true,
                    columns: $context->columns,
                    primaryKeys: $analysis->primaryKeys,
                );
            }
        }

        $isScalarLikeOperation = $context->operation === CacheOperation::Scalar
            || $context->operation === CacheOperation::PaginationCount;

        if (
            $isScalarLikeOperation
            && !$hasExplicit
            && $inferred->models === []
            && $inferred->tables === []
            && $dependencies->safe
            && (!empty($base->joins) || !empty($base->unions) || !is_string($base->from) || $base->from !== $builder->getModel()->getTable())
        ) {
            $dependencies = DependencySet::unsafe(['complex_query_requires_depends_on']);
            $bypassReasons['dependency'] = array_values(array_unique([
                ...($bypassReasons['dependency'] ?? []),
                'complex_query_requires_depends_on',
            ]));
        }

        if ($dependencies->safe && ($hasResultDependencies || $isResultStyleOperation)) {
            $isStrictRelation = in_array($context->operation, [CacheOperation::Pivot, CacheOperation::Through], true);

            if (!$isStrictRelation || empty($bypassReasons['normalization'])) {
                $this->checkDependencyCompleteness($analysis, $dependencies, $builder->getModel()->getTable());

                return new CachePlan(
                    mode: CacheMode::Result,
                    operation: $context->operation,
                    dependencies: $dependencies,
                    normalizable: $normalizable && empty($bypassReasons['normalization']),
                    columns: $context->columns,
                    primaryKeys: $analysis->primaryKeys,
                    reasons: $hasResultDependencies ? ['Using dependency result cache.'] : ['Using result cache.'],
                );
            }
        }

        if (!$dependencies->safe) {
            $bypassReasons['dependency'] = array_values(array_unique([
                ...($bypassReasons['dependency'] ?? []),
                ...$dependencies->reasons,
            ]));
        }

        return new CachePlan(
            mode: CacheMode::Bypass,
            operation: $context->operation,
            dependencies: $dependencies,
            normalizable: $normalizable,
            columns: $context->columns,
            primaryKeys: $analysis->primaryKeys,
            reasons: array_values(array_unique([
                ...$normalizationReasons,
                ...$dependencies->reasons,
                ...$analysis->dependencyReasons(),
                ...$analysis->normalizationReasons(),
                ...($bypassReasons['normalization'] ?? []),
            ])),
            bypassReasons: $bypassReasons,
        );
    }

    private function checkDependencyCompleteness(QueryAnalysis $analysis, DependencySet $dependencies, string $baseTable): void
    {
        if (!config('app.debug', false)) {
            return;
        }

        $queryTables = $analysis->tables ?? [];

        // strip connection prefix from table keys ("conn:table" → "table")
        $declaredTables = array_map(
            fn($key) => str_contains($key, ':') ? substr($key, strpos($key, ':') + 1) : $key,
            $dependencies->tables
        );

        // Map declared models to their tables
        foreach ($dependencies->models as $modelClass) {
            if (class_exists($modelClass) && is_subclass_of($modelClass, Model::class)) {
                $declaredTables[] = (new $modelClass)->getTable();
            }
        }

        // Add the base table to the declared list so it doesn't get flagged as missing
        $declaredTables[] = $baseTable;

        $missing = array_diff($queryTables, $declaredTables);

        if (!empty($missing)) {
            $tablesStr = implode(', ', $missing);
            Log::warning(
                "NormCache Warning: Query touches tables ({$tablesStr}) that are not present in dependsOn(). This is an under-declared dependency and can lead to stale cache reads."
            );
        }
    }

    private function resolveContextReasons(CacheableBuilder $builder, CachePlanContext $context): array
    {
        $reasons = $context->contextReasons;

        if ($builder->isCacheSkipped()) {
            $reasons['opted_out'][] = 'withoutCache() was called explicitly';
        }

        if (!NormCache::isEnabled()) {
            $reasons['opted_out'][] = 'cache is globally disabled';
        }

        if ($builder->getModel()->getConnection()->transactionLevel() > 0) {
            $reasons['safety'][] = 'inside a database transaction';
        }

        return $reasons;
    }
}
