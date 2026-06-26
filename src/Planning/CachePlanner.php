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
    private const SIMPLE_RESULT_BYPASS_FLAGS = QueryInspection::RAW_ORDER
        | QueryInspection::RAW_WHERE
        | QueryInspection::SUBQUERY_WHERE
        | QueryInspection::EXISTS_WHERE
        | QueryInspection::LOCK
        | QueryInspection::NON_CANONICAL_FROM
        | QueryInspection::JOIN
        | QueryInspection::UNION;

    // Raw ORDER is allowed: getCountForPagination() ignores ordering entirely.
    private const SIMPLE_PAGINATION_BYPASS_FLAGS = QueryInspection::RAW_WHERE
        | QueryInspection::SUBQUERY_WHERE
        | QueryInspection::EXISTS_WHERE
        | QueryInspection::LOCK
        | QueryInspection::NON_CANONICAL_FROM
        | QueryInspection::JOIN
        | QueryInspection::UNION
        | QueryInspection::GROUP
        | QueryInspection::HAVING;

    public function __construct(
        private readonly QueryAnalyzer $analyzer = new QueryAnalyzer,
    ) {}

    public function plan(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlanContext $context,
        PlanningMode $planningMode = PlanningMode::Hot,
    ): CachePlan {
        $model = $builder->getModel();
        $cacheSkipped = $builder->isCacheSkipped();
        $cacheDisabled = !NormCache::isEnabled();
        $explain = $planningMode === PlanningMode::Explain;

        if ($cacheSkipped || $cacheDisabled || isset($context->contextReasons['opted_out'])) {
            return $this->globalBypass($builder, $model, $context, $cacheSkipped, $cacheDisabled);
        }

        $insideTransaction = $model->getConnection()->transactionLevel() > 0;

        if ($insideTransaction && !$explain) {
            return $this->transactionBypass($builder, $model, $context);
        }

        return match ($context->operation) {
            CacheOperation::Scalar => $this->planScalarLike($builder, $model, $base, $context, $insideTransaction, $explain, self::SIMPLE_RESULT_BYPASS_FLAGS),
            CacheOperation::PaginationCount => $this->planScalarLike($builder, $model, $base, $context, $insideTransaction, $explain, self::SIMPLE_PAGINATION_BYPASS_FLAGS),
            CacheOperation::Pivot,
            CacheOperation::Through => $this->planRelationResult($builder, $model, $base, $context, $insideTransaction, $explain),
            CacheOperation::Models,
            CacheOperation::BelongsToEagerLoad,
            CacheOperation::MorphToEagerLoad => $this->planModels($builder, $model, $base, $context, $insideTransaction, $explain),
        };
    }

    private function globalBypass(
        CacheableBuilder $builder,
        Model $model,
        CachePlanContext $context,
        bool $cacheSkipped,
        bool $cacheDisabled,
    ): CachePlan {
        $reasons = $this->resolveContextReasons(
            $context->contextReasons,
            cacheSkipped: $cacheSkipped,
            cacheDisabled: $cacheDisabled,
            insideTransaction: false,
        )['opted_out'] ?? [];

        return CachePlan::bypass(
            operation: $context->operation,
            dependencies: $this->baseDependencies($builder, $model, $context),
            reasons: $reasons,
            bypassReasons: ['opted_out' => $reasons],
        );
    }

    private function transactionBypass(
        CacheableBuilder $builder,
        Model $model,
        CachePlanContext $context,
    ): CachePlan {
        $reasons = ['inside a database transaction'];

        return CachePlan::bypass(
            operation: $context->operation,
            dependencies: $this->baseDependencies($builder, $model, $context),
            reasons: $reasons,
            bypassReasons: ['safety' => $reasons],
        );
    }

    private function planScalarLike(
        CacheableBuilder $builder,
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        bool $insideTransaction,
        bool $explain,
        int $simpleBypassFlags,
    ): CachePlan {
        $explicitModels = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $hasExplicit = $builder->hasExplicitDependencies();

        if (!$explain && !$insideTransaction && !$hasExplicit && $context->contextReasons === []) {
            $plan = $this->trySimpleResultPlan($model, $base, $context, $simpleBypassFlags);

            if ($plan !== null) {
                return $plan;
            }
        }

        return $this->planInspectedResult(
            model: $model,
            base: $base,
            context: $context,
            explicitModels: $explicitModels,
            explicitTables: $explicitTables,
            hasExplicit: $hasExplicit,
            insideTransaction: $insideTransaction,
            explain: $explain,
            scalarLike: true,
        );
    }

    private function planRelationResult(
        CacheableBuilder $builder,
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        bool $insideTransaction,
        bool $explain,
    ): CachePlan {
        $explicitModels = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $hasExplicit = $builder->hasExplicitDependencies();

        return $this->planInspectedResult(
            model: $model,
            base: $base,
            context: $context,
            explicitModels: $explicitModels,
            explicitTables: $explicitTables,
            hasExplicit: $hasExplicit,
            insideTransaction: $insideTransaction,
            explain: $explain,
            strictRelation: true,
        );
    }

    private function planModels(
        CacheableBuilder $builder,
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        bool $insideTransaction,
        bool $explain,
    ): CachePlan {
        $modelClass = $model::class;
        $modelTable = $model->getTable();
        $inferred = $context->inferredDependencies;
        $explicitModels = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $hasExplicit = $builder->hasExplicitDependencies();

        $inspection = $this->analyzer->inspect(
            $base,
            $modelTable,
            $context->columns,
            [$model->getKeyName(), $model->getQualifiedKeyName()],
            includeTables: $explain,
        );

        if ($this->qualifiesForDirectModels($explain, $insideTransaction, $hasExplicit, $context, $inferred, $inspection)) {
            return CachePlan::direct(
                operation: $context->operation,
                dependencies: DependencySet::singleModel($modelClass),
                primaryKeys: $inspection->primaryKeys,
                columns: $context->columns,
            );
        }

        $hasDependencyBypass = $inspection->hasDependencyBypass();
        $hasContextDependencyBypass = isset($context->contextReasons['dependency']);

        $dependencies = $this->dependsOnPrimaryModelOnly($hasExplicit, $inferred, $hasDependencyBypass, $hasContextDependencyBypass)
            ? DependencySet::singleModel($modelClass)
            : $this->resolveDependencies(
                $modelClass,
                $context,
                $inspection,
                $explicitModels,
                $explicitTables,
                $hasExplicit,
            );

        if ($insideTransaction
            || $inspection->hasSafetyBypass()
            || isset($context->contextReasons['safety'])) {
            return $this->safetyBypass($context, $inspection, $dependencies, $insideTransaction);
        }

        $normalizable = !$hasDependencyBypass
            && !$hasContextDependencyBypass
            && $inspection->normalizationFlags() === 0
            && !isset($context->contextReasons['normalization']);
        $requiresPrimaryKeys = false;

        if ($context->operation === CacheOperation::BelongsToEagerLoad
            || $context->operation === CacheOperation::MorphToEagerLoad) {
            $requiresPrimaryKeys = $inspection->primaryKeys === null;
            $normalizable = $normalizable && !$requiresPrimaryKeys;
        }

        if ($normalizable && $dependencies->safe) {
            return CachePlan::normalized(
                operation: $context->operation,
                dependencies: $dependencies,
                columns: $context->columns,
                primaryKeys: $inspection->primaryKeys,
            );
        }

        if ($dependencies->safe && $this->hasResultDependencies($context, $hasExplicit)) {
            if ($this->requiresExplicitSelectForJoinResult($builder, $base, $context)) {
                $this->warnUnderDeclaredDependencies($modelTable, $base, $inspection, $dependencies);
                $reasons = ['join_result_requires_explicit_select'];

                return CachePlan::bypass(
                    operation: $context->operation,
                    dependencies: $dependencies,
                    reasons: $reasons,
                    bypassReasons: ['normalization' => $reasons],
                );
            }

            return $this->resultPlan($modelTable, $base, $context, $inspection, $dependencies, $normalizable);
        }

        return $this->bypassPlan(
            $context,
            $inspection,
            $dependencies,
            requiresPrimaryKeys: $requiresPrimaryKeys,
        );
    }

    private function planInspectedResult(
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        ?array $explicitModels,
        array $explicitTables,
        bool $hasExplicit,
        bool $insideTransaction,
        bool $explain,
        bool $scalarLike = false,
        bool $strictRelation = false,
    ): CachePlan {
        $modelClass = $model::class;
        $modelTable = $model->getTable();
        $inferred = $context->inferredDependencies;

        $inspection = $this->inspect($model, $base, $context, collectTables: $explain);
        $dependencies = $this->resolveDependencies(
            $modelClass,
            $context,
            $inspection,
            $explicitModels,
            $explicitTables,
            $hasExplicit,
        );

        if ($bypass = $this->safetyBypass($context, $inspection, $dependencies, $insideTransaction)) {
            return $bypass;
        }

        if ($scalarLike
            && !$hasExplicit
            && $inferred->hasNoDependencies()
            && $dependencies->safe
            && (!empty($base->joins)
                || !empty($base->unions)
                || !is_string($base->from)
                || $base->from !== $modelTable)
        ) {
            $dependencies = DependencySet::unsafe(['complex_query_requires_depends_on']);
        }

        $normalizationFlags = $inspection->normalizationFlags();
        $hasContextNormalizationBypass = isset($context->contextReasons['normalization']);

        if ($strictRelation
            && $normalizationFlags === QueryInspection::JOIN
            && (count($base->joins ?? []) === 1 || $hasExplicit)) {
            $normalizationFlags = 0;
        }

        if ($dependencies->safe
            && (!$strictRelation || ($normalizationFlags === 0 && !$hasContextNormalizationBypass))) {
            return $this->resultPlan($modelTable, $base, $context, $inspection, $dependencies);
        }

        return $this->bypassPlan(
            $context,
            $inspection,
            $dependencies,
            relaxedRelationNormalization: $strictRelation
                && $normalizationFlags === 0
                && !$hasContextNormalizationBypass,
        );
    }

    private function trySimpleResultPlan(
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        int $bypassFlags,
    ): ?CachePlan {
        $inferred = $context->inferredDependencies;

        if (!$inferred->safe || !$inferred->hasNoDependencies()) {
            return null;
        }

        if (($this->analyzer->flags($base, $model->getTable(), null) & $bypassFlags) !== 0) {
            return null;
        }

        return CachePlan::result(
            operation: $context->operation,
            dependencies: DependencySet::singleModel($model::class),
            columns: $context->columns,
        );
    }

    // Touched tables are only collected for explain/debug output, so $collectTables receives $explain.
    private function inspect(
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        bool $collectTables,
    ): QueryInspection {
        return $this->analyzer->inspect(
            $base,
            $model->getTable(),
            $context->columns,
            [],
            $collectTables,
        );
    }

    private function qualifiesForDirectModels(
        bool $explain,
        bool $insideTransaction,
        bool $hasExplicit,
        CachePlanContext $context,
        DependencySet $inferred,
        QueryInspection $inspection,
    ): bool {
        return !$explain
            && !$insideTransaction
            && !$hasExplicit
            && $context->contextReasons === []
            && $inferred->safe
            && $inferred->hasNoDependencies()
            && $inspection->primaryKeys !== null
            && $inspection->normalizationFlags() === 0
            && !$inspection->hasSafetyBypass();
    }

    private function dependsOnPrimaryModelOnly(
        bool $hasExplicit,
        DependencySet $inferred,
        bool $hasDependencyBypass,
        bool $hasContextDependencyBypass,
    ): bool {
        return !$hasExplicit
            && $inferred->safe
            && $inferred->hasNoDependencies()
            && !$hasDependencyBypass
            && !$hasContextDependencyBypass;
    }

    private function resolveDependencies(
        string $modelClass,
        CachePlanContext $context,
        ?QueryInspection $inspection,
        ?array $explicitModels,
        array $explicitTables,
        bool $hasExplicit,
    ): DependencySet {
        $inferred = $context->inferredDependencies;

        if ($hasExplicit) {
            return new DependencySet(
                models: array_keys(array_flip([
                    $modelClass,
                    ...$inferred->models,
                    ...($explicitModels ?? []),
                ])),
                tables: array_values(array_unique([...$inferred->tables, ...$explicitTables])),
            );
        }

        $hasDependencyBypass = $inspection !== null && $inspection->hasDependencyBypass();

        // EXISTS_WHERE-only bypasses are exempt if inferred dependencies are safe and non-empty.
        $exempt = $hasDependencyBypass
            && $inspection->hasOnlyExistsDependencyBypass()
            && $inferred->safe
            && !$inferred->hasNoDependencies();

        if (($hasDependencyBypass && !$exempt)
            || isset($context->contextReasons['dependency'])
            || !$inferred->safe) {
            return DependencySet::unsafe(array_values(array_unique([
                ...($inspection !== null ? BypassReasons::fromInspection($inspection)['dependency'] ?? [] : []),
                ...($context->contextReasons['dependency'] ?? []),
                ...$inferred->reasons,
            ])));
        }

        if ($inferred->hasNoDependencies()) {
            return DependencySet::singleModel($modelClass);
        }

        return new DependencySet(
            models: array_keys(array_flip([$modelClass, ...$inferred->models])),
            tables: $inferred->tables,
        );
    }

    // Dependencies for plans made without query inspection (global/transaction bypasses).
    private function baseDependencies(
        CacheableBuilder $builder,
        Model $model,
        CachePlanContext $context,
    ): DependencySet {
        return $this->resolveDependencies(
            $model::class,
            $context,
            null,
            $builder->explicitDependencies(),
            $builder->explicitTableDependencies(),
            $builder->hasExplicitDependencies(),
        );
    }

    private function safetyBypass(
        CachePlanContext $context,
        QueryInspection $inspection,
        DependencySet $dependencies,
        bool $insideTransaction,
    ): ?CachePlan {
        if (!$insideTransaction
            && !$inspection->hasSafetyBypass()
            && !isset($context->contextReasons['safety'])) {
            return null;
        }

        $bypassReasons = $this->mergedBypassReasons($context, $inspection, $insideTransaction);
        $reasons = $bypassReasons['safety'] ?? [];

        return CachePlan::bypass(
            operation: $context->operation,
            dependencies: $dependencies,
            reasons: $reasons,
            bypassReasons: ['safety' => $reasons],
        );
    }

    // SELECT * over a join pulls joined-table columns into hydration; require an explicit select.
    private function requiresExplicitSelectForJoinResult(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlanContext $context,
    ): bool {
        return $context->selectAll
            && !empty($base->joins)
            && empty($base->columns)
            && !$builder->hasAggregateColumns();
    }

    private function hasResultDependencies(CachePlanContext $context, bool $hasExplicit): bool
    {
        return $hasExplicit
            || !$context->inferredDependencies->hasNoDependencies();
    }

    private function resultPlan(
        string $modelTable,
        QueryBuilder $base,
        CachePlanContext $context,
        QueryInspection $inspection,
        DependencySet $dependencies,
        bool $normalizable = false,
    ): CachePlan {
        $this->warnUnderDeclaredDependencies($modelTable, $base, $inspection, $dependencies);

        return CachePlan::result(
            operation: $context->operation,
            dependencies: $dependencies,
            normalizable: $normalizable,
            columns: $context->columns,
            primaryKeys: $inspection->primaryKeys,
        );
    }

    private function bypassPlan(
        CachePlanContext $context,
        QueryInspection $inspection,
        DependencySet $dependencies,
        bool $requiresPrimaryKeys = false,
        bool $relaxedRelationNormalization = false,
    ): CachePlan {
        $bypassReasons = $this->mergedBypassReasons($context, $inspection);

        if ($requiresPrimaryKeys) {
            $bypassReasons['normalization'][] = 'eager load requires primary key lookup';
        }

        if ($relaxedRelationNormalization) {
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

    private function warnUnderDeclaredDependencies(
        string $modelTable,
        QueryBuilder $base,
        QueryInspection $inspection,
        DependencySet $dependencies,
    ): void {
        if (!config('app.debug', false)) {
            return;
        }

        if ($inspection->has(QueryInspection::EXISTS_WHERE | QueryInspection::SUBQUERY_WHERE)) {
            Log::warning(
                'NormCache Warning: Query contains subquery/exists predicates. NormCache cannot verify all touched tables; ensure dependsOn()/dependsOnTables() includes every table read by the subquery.'
            );
        }

        $this->checkDependencyCompleteness(
            $inspection->tables ?? $this->analyzer->extractTables($base, $modelTable),
            $dependencies,
            $modelTable,
        );
    }

    private function checkDependencyCompleteness(array $queryTables, DependencySet $dependencies, string $baseTable): void
    {
        // Strip connection prefix from table keys ("conn:table" → "table").
        $declaredTables = array_map(
            fn($key) => str_contains($key, ':') ? substr($key, strpos($key, ':') + 1) : $key,
            $dependencies->tables
        );

        // Map declared models to their tables.
        foreach ($dependencies->models as $modelClass) {
            if (class_exists($modelClass) && is_subclass_of($modelClass, Model::class)) {
                $declaredTables[] = (new $modelClass)->getTable();
            }
        }

        // Add the base table to the declared list so it doesn't get flagged as missing.
        $declaredTables[] = $baseTable;

        $missing = array_diff($queryTables, $declaredTables);

        if (!empty($missing)) {
            $tablesStr = implode(', ', $missing);
            Log::warning(
                "NormCache Warning: Query touches tables ({$tablesStr}) that are not present in dependsOn()/dependsOnTables(). This is an under-declared dependency and can lead to outdated cache reads."
            );
        }
    }

    private function mergedBypassReasons(
        CachePlanContext $context,
        QueryInspection $inspection,
        bool $insideTransaction = false,
    ): array {
        return BypassReasons::merge(
            $this->resolveContextReasons(
                $context->contextReasons,
                cacheSkipped: false,
                cacheDisabled: false,
                insideTransaction: $insideTransaction,
            ),
            BypassReasons::fromInspection($inspection),
        );
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
