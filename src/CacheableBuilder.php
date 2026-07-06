<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Arr;
use Illuminate\Support\LazyCollection;
use NormCache\Cache\ModelsExecutor;
use NormCache\Enums\CacheStrategy;
use NormCache\Enums\PlanningMode;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Planning\BypassReasons;
use NormCache\Planning\CachePlanner;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Relations\CachesRelationAggregates;
use NormCache\Relations\CachesRelationExistence;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Traits\Cacheable;
use NormCache\Traits\CachesScalarResults;
use NormCache\Traits\HandlesBuilderInvalidation;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\PreparedQuery;

class CacheableBuilder extends Builder
{
    use CachesRelationAggregates, CachesRelationExistence, CachesScalarResults, HandlesBuilderInvalidation;

    private static array $validatedModelClasses = [];

    private static ?CachePlanner $sharedPlanner = null;

    private static ?ModelsExecutor $sharedModelsExecutor = null;

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private ?array $dependsOn = null;

    private array $dependsOnTables = [];

    private ?string $cacheTag = null;

    private ?string $cacheSpace = null;

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    public function withoutCache(): static
    {
        $this->skipCache = true;

        return $this;
    }

    public function isCacheSkipped(): bool
    {
        return $this->skipCache;
    }

    public function ttl(int $ttl): static
    {
        if ($ttl <= 0) {
            throw new \InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->queryTtl = $ttl;

        return $this;
    }

    public function tag(string $tag): static
    {
        CacheKeyBuilder::assertValidTag($tag);

        $this->cacheTag = $tag;

        return $this;
    }

    public function space(string $name): static
    {
        $this->cacheSpace = $name;

        return $this;
    }

    public function getSpace(): ?string
    {
        return $this->cacheSpace;
    }

    public function dependsOn(array $modelClasses): static
    {
        if (empty($modelClasses)) {
            throw new \InvalidArgumentException('dependsOn() requires at least one model class.');
        }

        $existing = $this->dependsOn ?? [];

        foreach ($modelClasses as $class) {
            if (!is_string($class)) {
                throw new \InvalidArgumentException('dependsOn() expects model class names, not model instances.');
            }

            if (!isset(self::$validatedModelClasses[$class])) {
                if (!is_a($class, Model::class, true)) {
                    throw new \InvalidArgumentException("dependsOn() class [{$class}] must be an Eloquent model.");
                }

                if (!in_array(Cacheable::class, class_uses_recursive($class), true)) {
                    throw new \InvalidArgumentException(
                        "dependsOn() class [{$class}] must use the NormCache\\Cacheable trait."
                    );
                }

                self::$validatedModelClasses[$class] = true;
            }

            if (!in_array($class, $existing, true)) {
                $existing[] = $class;
            }
        }

        $this->dependsOn = $existing;

        return $this;
    }

    public function dependsOnTables(array $tables): static
    {
        if (empty($tables)) {
            throw new \InvalidArgumentException('dependsOnTables() requires at least one table name.');
        }

        foreach ($tables as $table) {
            if (!is_string($table) || $table === '') {
                throw new \InvalidArgumentException('dependsOnTables() expects non-empty table name strings.');
            }

            if (preg_match('/[:{}\s*]/', $table)) {
                throw new \InvalidArgumentException(
                    'dependsOnTables() table name must not contain reserved characters (: { } * or whitespace).'
                );
            }
        }

        $conn = $this->model->getConnection()->getName();
        $this->dependsOnTables = array_values(array_unique(array_merge(
            $this->dependsOnTables,
            array_map(fn($table) => "{$conn}:{$table}", $tables),
        )));

        return $this;
    }

    public function explicitDependencies(): ?array
    {
        return $this->dependsOn;
    }

    public function explicitTableDependencies(): array
    {
        return $this->dependsOnTables;
    }

    public function hasExplicitDependencies(): bool
    {
        return $this->dependsOn !== null || $this->dependsOnTables !== [];
    }

    public function getQueryTtl(): ?int
    {
        return $this->queryTtl;
    }

    public function getCacheTag(): ?string
    {
        return $this->cacheTag;
    }

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    public function explain(): string
    {
        $prepared = $this->prepareCacheExecution();
        $plan = $this->planPrepared($prepared, fn(DependencySet $inferred) => CachePlanContext::models(
            ProjectionClassifier::resolve($prepared->base, ['*']),
            $inferred,
            selectAll: true,
        ), PlanningMode::Explain);

        $space = ($plan->space !== null && $plan->space->name !== 'default')
            ? ' [space: ' . $plan->space->name . ']'
            : '';

        return match ($plan->strategy) {
            CacheStrategy::DirectModels => 'cached: direct (primary key)' . $space,
            CacheStrategy::NormalizedQuery => 'cached' . $space,
            CacheStrategy::VersionedResult => $this->explainResultStrategy() . $space,
            CacheStrategy::LiveQuery => $this->explainBypassStrategy($plan),
        };
    }

    private function explainResultStrategy(): string
    {
        $hasExplicit = $this->dependsOn !== null || $this->dependsOnTables !== [];

        return $hasExplicit ? 'cached: result (dependsOn())' : 'cached: result';
    }

    private function explainBypassStrategy(CachePlan $plan): string
    {
        $labels = BypassReasons::labels();
        $parts = [];
        foreach ($plan->bypassReasons as $category => $reasons) {
            $parts[] = ($labels[$category] ?? $category) . ': ' . implode(', ', $reasons);
        }

        return 'not cached — ' . implode(' | ', $parts);
    }

    public function get($columns = ['*']): Collection
    {
        if ($this->skipCache || !NormCache::isEnabled()) {
            return $this->getWithoutCache($columns);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $columns = Arr::wrap($columns);
        $prepared = $this->prepareCacheExecution();
        $model = $this->model::class;
        $base = $prepared->base;

        $plan = $this->planPrepared($prepared, fn(DependencySet $inferred) => CachePlanContext::models(
            ProjectionClassifier::resolve($base, $columns),
            $inferred,
            selectAll: $columns === ['*'],
        ));

        return NormCache::withSpace($plan->space, fn() => match ($plan->strategy) {
            CacheStrategy::DirectModels => NormCache::rescue(
                fn() => $this->modelsExecutor()->runDirect($prepared, $plan->primaryKeys, $model, $plan->columns, $this->model),
                fn() => $this->collectFromPrepared($prepared, $columns),
            ),
            CacheStrategy::NormalizedQuery => NormCache::rescue(
                fn() => $this->modelsExecutor()->runNormalized($prepared, $plan, $model, $plan->columns, $this->cacheTag, $this->queryTtl, $debugbarStart, $this->model),
                fn() => $this->collectFromPrepared($prepared, $columns)
            ),
            CacheStrategy::VersionedResult => $this->executeResultQuery($prepared, $plan, $columns),
            CacheStrategy::LiveQuery => $this->bypassAndReturn($model, $plan->bypassReasons, $debugbarStart, $prepared, $columns),
        });
    }

    private function executeResultQuery(
        PreparedQuery $prepared,
        CachePlan $plan,
        array $columns,
    ): Collection {
        $model = $this->model::class;

        [$payload, $cached] = NormCache::result()->execute(
            $prepared,
            $plan,
            ResultKind::Collection,
            $columns,
            function () use ($prepared, $columns) {
                if ($this->hasAggregateColumns()) {
                    $rawModels = $this->collectFromPrepared($prepared, $columns, false);

                    return $this->resultPayloadFromEloquentModels($rawModels);
                }

                return $this->buildResultPayloadFromQuery($prepared->baseWithColumns($columns));
            }
        );

        return $this->hydrateResultPayload($payload, $model, $cached, $prepared);
    }

    private function bypassAndReturn(
        string $model,
        array $bypassReasons,
        mixed $debugbarStart,
        PreparedQuery $prepared,
        array $columns,
    ): Collection {
        CacheReporter::queryBypassed($model, $bypassReasons, $debugbarStart);

        return $this->collectFromPrepared($prepared, $columns);
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($total !== null || $this->skipCache || !NormCache::isEnabled()) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $prepared = $this->prepareCacheExecution();
        $plan = $this->planPrepared($prepared, fn(DependencySet $inferred) => CachePlanContext::paginationCount($inferred));

        if ($plan->strategy === CacheStrategy::LiveQuery) {
            CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons, $debugbarStart);

            return parent::paginate($perPage, $columns, $pageName, $page);
        }

        try {
            $cachedTotal = NormCache::withSpace($plan->space, fn() => $this->rememberPaginationTotal($prepared, $plan));
        } catch (\Throwable $e) {
            NormCache::fallback($e);

            $cachedTotal = null;
        }

        return parent::paginate($perPage, $columns, $pageName, $page, $cachedTotal);
    }

    public function eagerLoadRelations(array $models): array
    {
        if (!$this->skipCache) {
            return parent::eagerLoadRelations($models);
        }

        $original = $this->eagerLoad;

        try {
            foreach ($this->eagerLoad as $name => $constraint) {
                $this->eagerLoad[$name] = function ($query) use ($constraint) {
                    $constraint($query);
                    $builder = $query instanceof EloquentRelation ? $query->getQuery() : $query;
                    if ($builder instanceof self) {
                        $builder->withoutCache();
                    }
                };
            }

            return parent::eagerLoadRelations($models);
        } finally {
            $this->eagerLoad = $original;
        }
    }

    public function sole($columns = ['*']): Model
    {
        return $this->bypassingCache(fn() => parent::sole($columns));
    }

    public function chunk($count, callable $callback): bool
    {
        return $this->bypassingCache(fn() => parent::chunk($count, $callback));
    }

    public function each(callable $callback, $count = 1000): bool
    {
        return $this->bypassingCache(fn() => parent::each($callback, $count));
    }

    public function lazy($chunkSize = 1000): LazyCollection
    {
        return $this->bypassingCache(fn() => parent::lazy($chunkSize));
    }

    private function bypassingCache(callable $fn): mixed
    {
        $previous = $this->skipCache;
        $this->skipCache = true;
        try {
            return $fn();
        } finally {
            $this->skipCache = $previous;
        }
    }

    // -------------------------------------------------------------------------
    // Infrastructure
    // -------------------------------------------------------------------------

    public function buildResultPayloadFromQuery(QueryBuilder $base): array
    {
        return $base->get()->map(fn($r) => (array) $r)->all();
    }

    public function hydrateResultPayload(
        array $payload,
        string $model,
        bool $cached,
        PreparedQuery $prepared,
    ): Collection {
        return $this->finalizeResult(NormCache::hydrateResult($payload, $this->model, $cached), $prepared);
    }

    public function finalizeResult(array $models, PreparedQuery $prepared): Collection
    {
        if ($models && $prepared->builder->getEagerLoads()) {
            $models = $prepared->builder->eagerLoadRelations($models);
        }

        return $prepared->applyAfterCallbacks($this->model->newCollection($models));
    }

    public function getWithoutCache($columns): Collection
    {
        return parent::get($columns);
    }

    public function hasAfterQueryCallbacks(): bool
    {
        return $this->afterQueryCallbacks !== [];
    }

    public function prepareCacheExecution(): PreparedQuery
    {
        return $this->prepareScopedQuery()->applyBeforeCallbacks();
    }

    public function prepareScopedQuery(): PreparedQuery
    {
        /** @var self $builder */
        $builder = $this->applyScopes();

        return new PreparedQuery($builder, $builder->getQuery());
    }

    public function collectFromPrepared(
        PreparedQuery $prepared,
        array $columns = ['*'],
        bool $applyAfterCallbacks = true,
        ?\Closure $beforeEagerLoad = null,
    ): Collection {
        $builder = $prepared->builder;
        $models = $builder->getModels($columns);

        if ($beforeEagerLoad !== null) {
            $beforeEagerLoad($models);
        }

        if (count($models) > 0) {
            $models = $builder->eagerLoadRelations($models);
        }

        $collection = $builder->getModel()->newCollection($models);

        return $applyAfterCallbacks
            ? $prepared->applyAfterCallbacks($collection)
            : $collection;
    }

    public function applyRemovedScopesTo(self $target): void
    {
        foreach ($this->removedScopes as $scope) {
            $target->withoutGlobalScope($scope);
        }
    }

    public function hasRemovedScopes(): bool
    {
        return !empty($this->removedScopes);
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    public function cachePlan(
        QueryBuilder $base,
        CachePlanContext $context,
        PlanningMode $planningMode = PlanningMode::Hot,
    ): CachePlan {
        return $this->planner()->plan($this, $base, $context, $planningMode);
    }

    /** @param  \Closure(DependencySet): CachePlanContext  $context */
    public function planPrepared(
        PreparedQuery $prepared,
        \Closure $context,
        PlanningMode $mode = PlanningMode::Hot,
    ): CachePlan {
        $builder = $prepared->builder;
        $base = $prepared->base;

        $joinDeps = !empty($base->joins)
            ? (new QueryAnalyzer)->inferJoinDependencies($base, $builder->getModel()->getConnection()->getName())
            : DependencySet::empty();

        return $builder->cachePlan($base, $context($builder->inferAggregateDependencies()->merge($joinDeps)), $mode);
    }

    public function planner(): CachePlanner
    {
        return self::$sharedPlanner ??= new CachePlanner;
    }

    private function modelsExecutor(): ModelsExecutor
    {
        return self::$sharedModelsExecutor ??= new ModelsExecutor;
    }

    // Drop memoized services on tenant/Octane resets; the planner caches app() lookups.
    public static function resetSharedState(): void
    {
        self::$sharedPlanner = null;
        self::$sharedModelsExecutor = null;
    }

    private function rememberPaginationTotal(PreparedQuery $prepared, CachePlan $plan): int
    {
        [$value] = NormCache::result()->execute(
            $prepared,
            $plan,
            ResultKind::PaginationCount,
            [],
            fn() => $prepared->base->getCountForPagination()
        );

        return (int) $value;
    }
}
