<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Illuminate\Database\Query\Builder as QueryBuilder;
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
    private const SIMPLE_RESULT_FAST_PATH_BLOCKERS = QueryInspection::RAW_ORDER
        | QueryInspection::RAW_WHERE
        | QueryInspection::SUBQUERY_WHERE
        | QueryInspection::EXISTS_WHERE
        | QueryInspection::LOCK
        | QueryInspection::NON_CANONICAL_FROM
        | QueryInspection::JOIN
        | QueryInspection::UNION;

    // Raw ORDER is allowed: getCountForPagination() ignores ordering entirely.
    private const SIMPLE_PAGINATION_FAST_PATH_BLOCKERS = QueryInspection::RAW_WHERE
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
        private readonly DependencyResolver $dependencies = new DependencyResolver,
        private readonly ?CachePlanSpaceValidator $spaceValidator = null,
    ) {}

    public function analyzer(): QueryAnalyzer
    {
        return $this->analyzer;
    }

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

        if ($base->useWritePdo) {
            return CachePlan::bypass(
                operation: $context->operation,
                dependencies: $this->dependencies->resolveBase($builder, $model, $context),
                bypassReasons: ['safety' => ['explicit write PDO read']],
            );
        }

        $connection = $model->getConnection();
        $insideTransaction = $connection->transactionLevel() > 0;

        if ($insideTransaction && !$explain) {
            return $this->transactionBypass($builder, $model, $context);
        }

        $inspection = $this->analyze($builder, $model, $base, $context, $explain, $connection);

        $plan = isset($inspection->contextReasons['opted_out'])
            ? CachePlan::bypass(
                operation: $context->operation,
                dependencies: $this->dependencies->resolveBase($builder, $model, $context),
                bypassReasons: ['opted_out' => $inspection->contextReasons['opted_out']],
            )
            : match ($context->operation) {
                CacheOperation::Scalar => $this->planInspectedResult($builder, $base, $context, $inspection, $insideTransaction, $explain, self::SIMPLE_RESULT_FAST_PATH_BLOCKERS),
                CacheOperation::PaginationCount => $this->planInspectedResult($builder, $base, $context, $inspection, $insideTransaction, $explain, self::SIMPLE_PAGINATION_FAST_PATH_BLOCKERS),
                CacheOperation::Pivot,
                CacheOperation::Through => $this->planInspectedResult($builder, $base, $context, $inspection, $insideTransaction, $explain, strictRelation: true),
                CacheOperation::Models => $this->planModels($builder, $model, $base, $context, $inspection, $insideTransaction, $explain),
            };

        return $this->spaceValidator()->validate($plan, $builder, $model, $explain);
    }

    public function applySpaceValidation(
        CachePlan $plan,
        CacheableBuilder $builder,
        Model $model,
        bool $explain = false,
    ): CachePlan {
        return $this->spaceValidator()->validate($plan, $builder, $model, $explain);
    }

    private function spaceValidator(): CachePlanSpaceValidator
    {
        return $this->spaceValidator ?? CachePlanSpaceValidator::standalone();
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
            dependencies: $this->dependencies->resolveBase($builder, $model, $context),
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
            dependencies: $this->dependencies->resolveBase($builder, $model, $context),
            bypassReasons: ['safety' => $reasons],
        );
    }

    private function planModels(
        CacheableBuilder $builder,
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        QueryInspection $inspection,
        bool $insideTransaction,
        bool $explain,
    ): CachePlan {
        $modelClass = $model::class;
        $modelTable = $model->getTable();
        $inferred = $inspection->dependencies;
        $explicitModels = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $hasExplicit = $builder->hasExplicitDependencies();

        if ($this->qualifiesForDirectModels($explain, $insideTransaction, $hasExplicit, $context, $inferred, $inspection)) {
            return CachePlan::direct(
                operation: $context->operation,
                dependencies: DependencySet::singleModel($modelClass),
                primaryKeys: $inspection->primaryKeys,
                columns: $context->columns,
            );
        }

        $hasDependencyBypass = $inspection->hasDependencyBypass();
        $hasContextDependencyBypass = isset($inspection->contextReasons['dependency']);

        $dependencies = $this->dependsOnPrimaryModelOnly($hasExplicit, $inferred, $hasDependencyBypass, $hasContextDependencyBypass)
            ? DependencySet::singleModel($modelClass)
            : $this->dependencies->resolve(
                $modelClass,
                $context,
                $inspection,
                $explicitModels,
                $explicitTables,
                $hasExplicit,
            );

        if ($insideTransaction
            || $inspection->hasSafetyBypass()) {
            return $this->safetyBypass($context, $inspection, $dependencies, $insideTransaction);
        }

        $normalizable = $inferred->hasNoDependencies()
            && !$hasDependencyBypass
            && !$hasContextDependencyBypass
            && $inspection->normalizationFlags() === 0
            && !isset($inspection->contextReasons['normalization']);

        if ($normalizable && $dependencies->safe) {
            return CachePlan::normalized(
                operation: $context->operation,
                dependencies: $dependencies,
                columns: $context->columns,
                primaryKeys: $inspection->primaryKeys,
            );
        }

        if ($dependencies->safe && $this->hasResultDependencies($inspection, $hasExplicit)) {
            if ($this->requiresExplicitSelectForJoinResult($builder, $base, $context)) {
                return CachePlan::bypass(
                    operation: $context->operation,
                    dependencies: $dependencies,
                    bypassReasons: ['normalization' => ['join_result_requires_explicit_select']],
                );
            }

            return $this->resultPlan($context, $inspection, $dependencies);
        }

        return $this->bypassPlan($context, $inspection, $dependencies);
    }

    private function planInspectedResult(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlanContext $context,
        QueryInspection $inspection,
        bool $insideTransaction,
        bool $explain,
        ?int $simpleBypassFlags = null,
        bool $strictRelation = false,
    ): CachePlan {
        $model = $builder->getModel();
        $modelClass = $model::class;
        $modelTable = $model->getTable();
        $inferred = $inspection->dependencies;
        $explicitModels = $builder->explicitDependencies();
        $explicitTables = $builder->explicitTableDependencies();
        $hasExplicit = $builder->hasExplicitDependencies();

        if ($simpleBypassFlags !== null
            && !$explain
            && !$insideTransaction
            && !$hasExplicit
            && $inspection->contextReasons === []
            && ($plan = $this->trySimpleResultPlan($model, $context, $inspection, $simpleBypassFlags)) !== null) {
            return $plan;
        }

        $dependencies = $this->dependencies->resolve(
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

        if ($simpleBypassFlags !== null
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
        $hasContextNormalizationBypass = isset($inspection->contextReasons['normalization']);

        if ($strictRelation
            && $normalizationFlags === QueryInspection::JOIN
            && (count($base->joins ?? []) === 1 || $hasExplicit)) {
            $normalizationFlags = 0;
        }

        if ($dependencies->safe
            && (!$strictRelation || ($normalizationFlags === 0 && !$hasContextNormalizationBypass))) {
            return $this->resultPlan($context, $inspection, $dependencies);
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
        CachePlanContext $context,
        QueryInspection $inspection,
        int $bypassFlags,
    ): ?CachePlan {
        if (!$inspection->dependencies->safe || !$inspection->dependencies->hasNoDependencies()) {
            return null;
        }

        if (($inspection->flags & $bypassFlags) !== 0) {
            return null;
        }

        return CachePlan::result(
            operation: $context->operation,
            dependencies: DependencySet::singleModel($model::class),
            columns: $context->columns,
        );
    }

    private function analyze(
        CacheableBuilder $builder,
        Model $model,
        QueryBuilder $base,
        CachePlanContext $context,
        bool $explain,
        Connection $connection,
    ): QueryInspection {
        $primaryKeys = $context->operation === CacheOperation::Models
            ? [$model->getKeyName(), $model->getQualifiedKeyName()]
            : [];

        $capturedContextReasons = $builder->capturedContextReasons();
        $contextReasons = $context->contextReasons === [] && $capturedContextReasons === []
            ? []
            : BypassReasons::merge($context->contextReasons, $capturedContextReasons);

        $table = $model->getTable();
        $softDeleteScopeColumn = $context->operation === CacheOperation::Models
            ? $this->activeSoftDeleteScopeColumn($builder, $model)
            : null;

        $allowPrimaryKeyFastPath = $context->operation === CacheOperation::Models
            && !$explain
            && !$builder->hasExplicitDependencies();

        return $this->analyzer->inspect(
            $base,
            $table,
            $context->columns,
            $primaryKeys,
            $softDeleteScopeColumn,
            fn(): string => $connection->getName() ?? $model->getConnectionName() ?? '',
            $builder->capturedDependencies(),
            $contextReasons,
            $builder->capturedOpaqueJoins(),
            $builder->hasCapturedOpaqueFrom(),
            $builder->capturedOpaqueWhereSubqueries(),
            $allowPrimaryKeyFastPath,
        );
    }

    private function activeSoftDeleteScopeColumn(CacheableBuilder $builder, Model $model): ?string
    {
        if (!$model::hasGlobalScope(SoftDeletingScope::class)
            || in_array(SoftDeletingScope::class, $builder->removedScopes(), true)
            || !method_exists($model, 'getQualifiedDeletedAtColumn')) {
            return null;
        }

        return $model->getQualifiedDeletedAtColumn();
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
            && $inspection->contextReasons === []
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

    private function safetyBypass(
        CachePlanContext $context,
        QueryInspection $inspection,
        DependencySet $dependencies,
        bool $insideTransaction,
    ): ?CachePlan {
        if (!$insideTransaction
            && !$inspection->hasSafetyBypass()) {
            return null;
        }

        $bypassReasons = $this->mergedBypassReasons($inspection, $insideTransaction);

        return CachePlan::bypass(
            operation: $context->operation,
            dependencies: $dependencies,
            bypassReasons: ['safety' => $bypassReasons['safety'] ?? []],
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

    private function hasResultDependencies(QueryInspection $inspection, bool $hasExplicit): bool
    {
        return $hasExplicit || !$inspection->dependencies->hasNoDependencies();
    }

    private function resultPlan(
        CachePlanContext $context,
        QueryInspection $inspection,
        DependencySet $dependencies,
    ): CachePlan {
        return CachePlan::result(
            operation: $context->operation,
            dependencies: $dependencies,
            columns: $context->columns,
            primaryKeys: $inspection->primaryKeys,
        );
    }

    private function bypassPlan(
        CachePlanContext $context,
        QueryInspection $inspection,
        DependencySet $dependencies,
        bool $relaxedRelationNormalization = false,
    ): CachePlan {
        $bypassReasons = $this->mergedBypassReasons($inspection);

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
            bypassReasons: $bypassReasons,
        );
    }

    private function mergedBypassReasons(
        QueryInspection $inspection,
        bool $insideTransaction = false,
    ): array {
        return BypassReasons::merge(
            $insideTransaction ? ['safety' => ['inside a database transaction']] : [],
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
