<?php

namespace NormCache\Planning;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Facades\Log;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheOperation;
use NormCache\Enums\PlanningMode;
use NormCache\Facades\NormCache;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Spaces\CacheSpaceResolver;
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
        private readonly DependencyResolver $dependencies = new DependencyResolver,
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

        $plan = match ($context->operation) {
            CacheOperation::Scalar => $this->planScalarLike($builder, $model, $base, $context, $insideTransaction, $explain, self::SIMPLE_RESULT_BYPASS_FLAGS),
            CacheOperation::PaginationCount => $this->planScalarLike($builder, $model, $base, $context, $insideTransaction, $explain, self::SIMPLE_PAGINATION_BYPASS_FLAGS),
            CacheOperation::Pivot,
            CacheOperation::Through => $this->planRelationResult($builder, $model, $base, $context, $insideTransaction, $explain),
            CacheOperation::Models,
            CacheOperation::BelongsToEagerLoad,
            CacheOperation::MorphToEagerLoad => $this->planModels($builder, $model, $base, $context, $insideTransaction, $explain),
        };

        return $this->applySpaceValidation($plan, $builder, $model, $context->operation, $explain);
    }

    private ?CacheSpaceResolver $spaceResolver = null;

    private ?CacheSpaceRegistry $spaceRegistry = null;

    // Bypass (or throw) when a plan's dependencies don't co-locate in its cache space.
    // Neutral for default-only apps: everything resolves to the default space.
    private function applySpaceValidation(
        CachePlan $plan,
        CacheableBuilder $builder,
        Model $model,
        CacheOperation $operation,
        bool $explain = false,
    ): CachePlan {
        if (!$plan->isCacheable()) {
            return $plan;
        }

        $resolver = $this->spaceResolver ??= app(CacheSpaceResolver::class);
        $registry = $this->spaceRegistry ??= app(CacheSpaceRegistry::class);

        $space = $resolver->resolve($model::class, $builder->getSpace());
        $validation = $registry->validateDependencies(
            $space,
            $plan->dependencies->models,
            $plan->dependencies->tables,
        );

        if ($validation->ok) {
            return $plan->withSpace($space);
        }

        $offending = implode(', ', [...$validation->invalidModels, ...$validation->invalidTables]);
        $reasons = ['cross-space dependencies for space [' . $space->name . ']: ' . $offending];

        if (!$explain && config('app.debug', false)) {
            $modelClass = $model::class;
            Log::warning(
                "NormCache: query for [{$modelClass}] in space [{$space->name}] depends on [{$offending}] "
                . "which are not in that space; the query will not cache. Add them to the space or drop the dependency."
            );
        }

        if (!$explain && config('normcache.spaces.cross_space_behavior', 'bypass') === 'throw') {
            throw new \RuntimeException('NormCache: ' . $reasons[0]);
        }

        return CachePlan::bypass(
            operation: $operation,
            dependencies: $plan->dependencies,
            reasons: $reasons,
            bypassReasons: ['dependency' => $reasons],
        )->withSpace($space);
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
            dependencies: $this->dependencies->resolveBase($builder, $model, $context),
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
            : $this->dependencies->resolve(
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
                $this->dependencies->warnUnderDeclared($modelTable, $base, $inspection, $dependencies);
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
        $this->dependencies->warnUnderDeclared($modelTable, $base, $inspection, $dependencies);

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
