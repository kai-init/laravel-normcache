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
use NormCache\Enums\CacheStatus;
use NormCache\Enums\CacheStrategy;
use NormCache\Enums\PlanningMode;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Planning\BypassReasons;
use NormCache\Planning\CachePlanner;
use NormCache\Relations\CachesRelationAggregates;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;
use NormCache\Traits\Cacheable;
use NormCache\Traits\CachesScalarResults;
use NormCache\Traits\HandlesBuilderInvalidation;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\PreparedQuery;

class CacheableBuilder extends Builder
{
    use CachesRelationAggregates, CachesScalarResults, HandlesBuilderInvalidation;

    private static array $validatedModelClasses = [];

    private static ?CachePlanner $sharedPlanner = null;

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private ?array $dependsOn = null;

    private array $dependsOnTables = [];

    private ?string $cacheTag = null;

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

    public function dependsOn(array $modelClasses): static
    {
        if (empty($modelClasses)) {
            throw new \InvalidArgumentException('dependsOn() requires at least one model class.');
        }

        foreach ($modelClasses as $class) {
            if (!is_string($class)) {
                throw new \InvalidArgumentException('dependsOn() expects model class names, not model instances.');
            }

            if (isset(self::$validatedModelClasses[$class])) {
                continue;
            }

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

        $this->dependsOn = $modelClasses;

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
        $executionBuilder = $prepared->builder;
        $base = $prepared->base;
        $resolvedCols = ProjectionClassifier::resolve($base, ['*']);
        $plan = $executionBuilder->cachePlan($base, CachePlanContext::models(
            $resolvedCols,
            $executionBuilder->inferAggregateDependencies(),
            selectAll: true,
        ), PlanningMode::Explain);

        return match ($plan->strategy) {
            CacheStrategy::DirectModels => 'cached: direct (primary key)',
            CacheStrategy::NormalizedQuery => 'cached',
            CacheStrategy::VersionedResult => $this->explainResultStrategy(),
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

        $plan = $prepared->builder->cachePlan($prepared->base, CachePlanContext::models(
            ProjectionClassifier::resolve($prepared->base, $columns),
            $prepared->builder->inferAggregateDependencies(),
            selectAll: $columns === ['*'],
        ));

        return match ($plan->strategy) {
            CacheStrategy::DirectModels => NormCache::rescue(
                fn() => $this->getModelsByIds($prepared, $plan->primaryKeys, $model, $plan->columns),
                fn() => $this->getWithoutCacheFromPrepared($prepared, $columns),
            ),
            CacheStrategy::NormalizedQuery => NormCache::rescue(
                fn() => $this->getFromCacheableQuery($prepared, $model, $plan->columns, $plan, $debugbarStart),
                fn() => $this->getWithoutCacheFromPrepared($prepared, $columns)
            ),
            CacheStrategy::VersionedResult => $this->executeResultQuery($prepared, $plan, $columns),
            CacheStrategy::LiveQuery => $this->bypassAndReturn($model, $plan->bypassReasons, $debugbarStart, $prepared, $columns),
        };
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
                    $rawModels = $this->getWithoutCacheFromPrepared($prepared, $columns, false);

                    return $this->resultPayloadFromEloquentModels($rawModels);
                }

                $resultBase = clone $prepared->base;
                if (empty($resultBase->columns) && $columns !== ['*']) {
                    $resultBase->columns = $columns;
                }

                return $this->buildResultPayloadFromQuery($resultBase);
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

        return $this->getWithoutCacheFromPrepared($prepared, $columns);
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($total !== null || $this->skipCache || !NormCache::isEnabled()) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $prepared = $this->prepareCacheExecution();
        $plan = $prepared->builder->cachePlan($prepared->base, CachePlanContext::paginationCount(
            $prepared->builder->inferAggregateDependencies()
        ));

        if ($plan->strategy === CacheStrategy::LiveQuery) {
            CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons, $debugbarStart);

            return parent::paginate($perPage, $columns, $pageName, $page);
        }

        try {
            $cachedTotal = $this->rememberPaginationTotal($prepared->base, $plan, $debugbarStart);
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
        // sole() must verify row count against live DB state, not a cached snapshot.
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

    public function cursor(): LazyCollection
    {
        // Streams via QueryBuilder::cursor() — never reaches CacheableBuilder::get().
        return parent::cursor();
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

    private function getWithoutCacheFromPrepared(
        PreparedQuery $prepared,
        array $columns,
        bool $applyAfterCallbacks = true,
    ): Collection {
        $builder = $prepared->builder;
        $models = $builder->getModels($columns);

        if (count($models) > 0) {
            $models = $builder->eagerLoadRelations($models);
        }

        $collection = $this->model->newCollection($models);

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

    private function planner(): CachePlanner
    {
        return self::$sharedPlanner ??= new CachePlanner;
    }

    private function getFromCacheableQuery(
        PreparedQuery $prepared,
        string $model,
        ?array $selectedCols,
        CachePlan $plan,
        mixed $debugbarStart,
    ): Collection {
        $executionBuilder = $prepared->builder;
        $base = $prepared->base;
        $hash = QueryHasher::forNormalizedQuery($executionBuilder, $base);
        $depClasses = $plan->dependencies->depClassesFor($model);
        $depTableKeys = $plan->dependencies->tables;

        return NormCache::engine()->runNormalized(
            fetch: fn() => NormCache::getModelsFromQuery($model, $hash, $this->cacheTag, $depClasses, $depTableKeys),
            waitForBuild: fn() => NormCache::waitForQueryBuild($model, $hash, $this->cacheTag, $depClasses, $depTableKeys),
            onBuild: function () use ($prepared, $executionBuilder, $base, $model, $selectedCols, $debugbarStart) {
                CacheReporter::queryMiss($model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $this->finalizeResult(NormCache::getModels($this->buildIds($base), $model, $selectedCols, null, $executionBuilder, true, $this->model), $prepared);
            },
            onMiss: function ($result) use ($prepared, $executionBuilder, $base, $model, $selectedCols, $debugbarStart) {
                CacheReporter::queryMiss($model, $result->key, $debugbarStart, ['kind' => 'ids']);

                $ids = $this->resolveIds($result->key, $base, $result->buildingKey, $result->versionKeys, $result->expectedVersions, $result->buildingToken);

                return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $executionBuilder, true, $this->model), $prepared);
            },
            onHit: function ($result) use ($prepared, $executionBuilder, $model, $hash, $selectedCols, $debugbarStart) {
                $key = $result->status === CacheStatus::Stale ? "stale:{$hash}" : $result->key;

                CacheReporter::queryHit($model, $key, $debugbarStart, [
                    'kind' => 'ids + models',
                    'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result->ids) . ' ids)',
                    'contains_model' => $result->ids,
                ]);

                return $this->finalizeResult(NormCache::getModels($result->ids, $model, $selectedCols, $result->models, $executionBuilder, true, $this->model), $prepared);
            },
        );
    }

    private function getModelsByIds(
        PreparedQuery $prepared,
        array $ids,
        string $model,
        ?array $selectedCols,
    ): Collection {
        $executionBuilder = $prepared->builder;

        return $this->finalizeResult(NormCache::getModels(
            $ids,
            $model,
            $selectedCols,
            null,
            $executionBuilder,
            false,
            $this->model
        ), $prepared);
    }

    private function resolveIds(string $key, QueryBuilder $base, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): array
    {
        CacheReporter::queryMiss($this->model::class, $key, null);

        $ids = $this->buildIds($base);
        NormCache::storeQueryIds($key, $ids, $this->queryTtl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken);

        return $ids;
    }

    private function buildIds(QueryBuilder $base): array
    {
        return $base
            ->cloneWithout(['columns'])
            ->cloneWithoutBindings(['select'])
            ->select($this->model->getKeyName())
            ->pluck($this->model->getKeyName())
            ->all();
    }

    private function rememberPaginationTotal(QueryBuilder $base, CachePlan $plan, mixed $debugbarStart): int
    {
        $model = $this->model::class;
        $depClasses = $plan->dependencies->depClassesFor($model);
        $depTableKeys = $plan->dependencies->tables;
        $hash = QueryHasher::forPaginationCountQuery($this, $base);

        return (int) NormCache::engine()->runScalar(
            fetch: fn() => NormCache::getResultCache($model, $depClasses, $hash, $this->cacheTag, $depTableKeys, CacheKeyBuilder::K_COUNT),
            waitForBuild: fn() => NormCache::waitForResultBuild($model, $hash, tag: $this->cacheTag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: CacheKeyBuilder::K_COUNT),
            compute: fn() => $base->getCountForPagination(),
            onStore: function ($value, $result) use ($model, $debugbarStart) {
                CacheReporter::queryMiss($model, $result->key, $debugbarStart, ['kind' => 'pagination count']);

                NormCache::storeResultCache(
                    $result->key,
                    [$value],
                    $result->buildingKey,
                    $this->queryTtl,
                    $result->wakeKey,
                    $result->versionKeys,
                    $result->expectedVersions,
                    $result->buildingToken
                );
            },
            onHit: function ($result) use ($model, $base, $debugbarStart) {
                if (!is_array($result->payload) || !array_key_exists(0, $result->payload)) {
                    return $base->getCountForPagination();
                }

                CacheReporter::queryHit($model, $result->key, $debugbarStart, ['kind' => 'pagination count']);

                return $result->payload[0];
            },
        );
    }
}
