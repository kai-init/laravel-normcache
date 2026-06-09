<?php

namespace NormCache\Planning;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Facades\Log;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheOperation;
use NormCache\Enums\PlanningMode;
use NormCache\Facades\NormCache;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\QueryInspection;

final class CachePlanner
{
    public function __construct(
        private readonly QueryAnalyzer $analyzer = new QueryAnalyzer,
    ) {}

    public function plan(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlanContext $context,
        PlanningMode $planningMode = PlanningMode::Hot,
    ): CachePlan {
        $slotting = NormCache::isSlotting();
        $model = $builder->getModel();
        $modelClass = $model::class;
        $modelTable = $model->getTable();
        $inferred = $context->inferredDependencies;
        $explicit = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $hasExplicit = $explicit !== null || $explicitTables !== [];
        $cacheSkipped = $builder->isCacheSkipped();
        $cacheDisabled = !NormCache::isEnabled();
        $insideTransaction = $model->getConnection()->transactionLevel() > 0;
        $includeTables = $planningMode === PlanningMode::Explain;
        $contextReasons = $context->contextReasons;

        if (!$cacheSkipped && !$cacheDisabled && !$insideTransaction && $context->requiresNormalization()
            && $inferred->safe && $inferred->models === [] && $inferred->tables === [] && !$hasExplicit) {
            $directIds = $this->analyzer->directPrimaryKeys(
                $base,
                $modelTable,
                $context->columns,
                [$model->getKeyName(), $model->getQualifiedKeyName()],
            );

            if ($directIds !== null) {
                return CachePlan::direct(
                    operation: $context->operation,
                    dependencies: DependencySet::singleModel($modelClass),
                    primaryKeys: $directIds,
                );
            }
        }

        $inspection = $this->analyzer->inspect(
            $base,
            $modelTable,
            $context->columns,
            $context->requiresNormalization()
                ? [$model->getKeyName(), $model->getQualifiedKeyName()]
                : [],
            $includeTables,
        );

        // 1. Resolve Dependencies
        $dependencies = $hasExplicit
            ? new DependencySet(
                models: array_keys(array_flip([$modelClass, ...$inferred->models, ...($explicit ?? [])])),
                tables: array_values(array_unique([...$inferred->tables, ...$explicitTables])),
            )
            : ($inspection->hasDependencyBypass() || isset($contextReasons['dependency']) || !$inferred->safe
                ? DependencySet::unsafe(array_values(array_unique([
                    ...(BypassReasons::fromInspection($inspection)['dependency'] ?? []),
                    ...($contextReasons['dependency'] ?? []),
                    ...$inferred->reasons,
                    ...($inspection->hasDependencyBypass() ? ['Query requires explicit dependencies.'] : []),
                ])))
                : ($inferred->models === [] && $inferred->tables === []
                    ? DependencySet::singleModel($modelClass)
                    : new DependencySet(
                        models: array_keys(array_flip([$modelClass, ...$inferred->models])),
                        tables: $inferred->tables,
                    )));

        // 2. Qualify Normalization
        $normalizable = false;
        $normalizationFlags = $inspection->normalizationFlags();
        $hasContextNormalizationBypass = isset($contextReasons['normalization']);
        $requiresPrimaryKeys = false;

        if ($context->requiresNormalization()) {
            $normalizable = !$insideTransaction
                && !$inspection->hasSafetyBypass()
                && !isset($contextReasons['safety'])
                && !$inspection->hasDependencyBypass()
                && !isset($contextReasons['dependency'])
                && $normalizationFlags === 0
                && !$hasContextNormalizationBypass;
        }

        // 3. Resolve Cache Mode
        $hasResultDependencies = $hasExplicit || $inferred->models !== [] || $inferred->tables !== [];
        $isResultStyleOperation = match ($context->operation) {
            CacheOperation::Scalar,
            CacheOperation::PaginationCount,
            CacheOperation::Pivot,
            CacheOperation::Through => true,
            default => false,
        };

        if ($cacheSkipped || $cacheDisabled || isset($contextReasons['opted_out'])) {
            $optedOutReasons = $this->resolveContextReasons(
                $contextReasons,
                cacheSkipped: $cacheSkipped,
                cacheDisabled: $cacheDisabled,
                insideTransaction: $insideTransaction,
            )['opted_out'] ?? [];

            return CachePlan::bypass(
                operation: $context->operation,
                dependencies: $dependencies,
                reasons: $optedOutReasons,
                bypassReasons: ['opted_out' => $optedOutReasons],
            );
        }

        // Relation-specific bypass relaxations or strictness
        if ($context->operation === CacheOperation::BelongsToEagerLoad || $context->operation === CacheOperation::MorphToEagerLoad) {
            if ($inspection->primaryKeys === null) {
                $requiresPrimaryKeys = true;
                $normalizable = false;
            }
        }

        if ($context->operation === CacheOperation::Pivot || $context->operation === CacheOperation::Through) {
            // Pivot/Through operations allow exactly one join (to the intermediate table)
            if ($normalizationFlags === QueryInspection::JOIN && count($base->joins ?? []) === 1) {
                $normalizationFlags = 0;
            }
        }

        if ($insideTransaction || $inspection->hasSafetyBypass() || isset($contextReasons['safety'])) {
            $bypassReasons = BypassReasons::merge(
                $this->resolveContextReasons(
                    $contextReasons,
                    cacheSkipped: false,
                    cacheDisabled: false,
                    insideTransaction: $insideTransaction,
                ),
                BypassReasons::fromInspection($inspection),
            );
            $safetyReasons = $bypassReasons['safety'] ?? [];

            return CachePlan::bypass(
                operation: $context->operation,
                dependencies: $dependencies,
                reasons: $safetyReasons,
                bypassReasons: ['safety' => $safetyReasons],
            );
        }

        if ($normalizable && $dependencies->safe) {
            $isMultiDependency = count($dependencies->models) + count($dependencies->tables) > 1;

            if (!$slotting || !$isMultiDependency) {
                return CachePlan::normalized(
                    operation: $context->operation,
                    dependencies: $dependencies,
                    columns: $context->columns,
                    primaryKeys: $inspection->primaryKeys,
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
            && (!empty($base->joins) || !empty($base->unions) || !is_string($base->from) || $base->from !== $modelTable)
        ) {
            $dependencies = DependencySet::unsafe(['complex_query_requires_depends_on']);
        }

        if ($dependencies->safe && ($hasResultDependencies || $isResultStyleOperation)) {
            $isStrictRelation = $context->operation === CacheOperation::Pivot
                || $context->operation === CacheOperation::Through;

            if (!$isStrictRelation || ($normalizationFlags === 0 && !$hasContextNormalizationBypass)) {
                if (config('app.debug', false)) {
                    $this->checkDependencyCompleteness(
                        $inspection->tables ?? $this->analyzer->extractTables($base, $modelTable),
                        $dependencies,
                        $modelTable,
                    );
                }

                return CachePlan::result(
                    operation: $context->operation,
                    dependencies: $dependencies,
                    normalizable: $normalizable,
                    columns: $context->columns,
                    primaryKeys: $inspection->primaryKeys,
                );
            }
        }

        $bypassReasons = BypassReasons::merge(
            $this->resolveContextReasons(
                $contextReasons,
                cacheSkipped: false,
                cacheDisabled: false,
                insideTransaction: false,
            ),
            BypassReasons::fromInspection($inspection),
        );

        if ($requiresPrimaryKeys) {
            $bypassReasons['normalization'][] = 'eager load requires primary key lookup';
        }

        if (($context->operation === CacheOperation::Pivot || $context->operation === CacheOperation::Through)
            && $normalizationFlags === 0
            && !$hasContextNormalizationBypass) {
            unset($bypassReasons['normalization']);
        }

        if (!$dependencies->safe) {
            $bypassReasons['dependency'] = array_values(array_unique([
                ...($bypassReasons['dependency'] ?? []),
                ...$dependencies->reasons,
            ]));
        }

        return CachePlan::bypass(
            operation: $context->operation,
            dependencies: $dependencies,
            reasons: array_values(array_unique([
                ...$dependencies->reasons,
                ...($bypassReasons['dependency'] ?? []),
                ...($bypassReasons['normalization'] ?? []),
            ])),
            bypassReasons: $bypassReasons,
        );
    }

    private function checkDependencyCompleteness(array $queryTables, DependencySet $dependencies, string $baseTable): void
    {
        if (!config('app.debug', false)) {
            return;
        }

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

    private function resolveContextReasons(
        array $reasons,
        bool $cacheSkipped,
        bool $cacheDisabled,
        bool $insideTransaction,
    ): array {
        if ($cacheSkipped) {
            $reasons['opted_out'][] = 'withoutCache() was called explicitly';
        }

        if ($cacheDisabled) {
            $reasons['opted_out'][] = 'cache is globally disabled';
        }

        if ($insideTransaction) {
            $reasons['safety'][] = 'inside a database transaction';
        }

        return array_filter($reasons);
    }
}
