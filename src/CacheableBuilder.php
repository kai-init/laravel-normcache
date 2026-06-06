<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\LazyCollection;
use NormCache\Enums\CacheMode;
use NormCache\Enums\CacheStatus;
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
        if (preg_match('/[:{}\s*]/', $tag)) {
            throw new \InvalidArgumentException('Cache tag must not contain reserved characters (: { } * or whitespace).');
        }

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
        $base = $this->toBase();
        $resolvedCols = ProjectionClassifier::resolve($base, ['*']);
        $plan = $this->cachePlan($base, CachePlanContext::models($resolvedCols, $this->inferAggregateDependencies()));

        if ($plan->mode === CacheMode::Normalized) {
            return $plan->primaryKeys !== null ? 'cached: direct (primary key)' : 'cached';
        }

        if ($plan->mode === CacheMode::Result) {
            if (!empty($base->joins) && empty($base->columns)) {
                return 'not cached — ' . BypassReasons::labels()['normalization'] . ': join_result_requires_explicit_select';
            }

            $hasExplicit = $this->dependsOn !== null || $this->dependsOnTables !== [];

            return $hasExplicit ? 'cached: result (dependsOn())' : 'cached: result';
        }

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

        $base = $this->toBase();
        $resolvedCols = ProjectionClassifier::resolve($base, (array) $columns);
        $model = $this->model::class;
        $plan = $this->cachePlan($base, CachePlanContext::models($resolvedCols, $this->inferAggregateDependencies()));

        if ($plan->mode === CacheMode::Normalized) {
            return NormCache::rescue(
                fn() => $this->getFromCacheableQuery($base, $model, $resolvedCols, $plan, $debugbarStart),
                fn() => $this->getWithoutCache($columns)
            );
        }

        if ($plan->mode === CacheMode::Result) {
            $usesEloquentResult = $this->hasAggregateColumns();
            $depClasses = $plan->dependencies->depClassesFor($model);
            $depTableKeys = $plan->dependencies->tables;

            if (!$usesEloquentResult && !empty($base->joins) && empty($base->columns)) {
                if ($columns === ['*']) {
                    CacheReporter::queryBypassed($model, ['normalization' => ['join_result_requires_explicit_select']], $debugbarStart);

                    return $this->getWithoutCache($columns);
                }

                $base->columns = (array) $columns;
            }

            if ($base->columns === null && $columns !== ['*']) {
                $base->columns = (array) $columns;
            }

            $hash = QueryHasher::forResultQuery($this, $base);

            return NormCache::rescue(
                fn() => NormCache::executor()->runResult(
                    fetch: fn() => NormCache::getResultCache($model, $depClasses, $hash, $this->cacheTag, $depTableKeys),
                    waitForBuild: fn() => NormCache::waitForBuild('result', $model, $hash, tag: $this->cacheTag, depClasses: $depClasses, depTableKeys: $depTableKeys),
                    onMiss: function ($result) use ($columns, $model, $base, $usesEloquentResult, $debugbarStart) {
                        CacheReporter::queryMiss($model, $result->key, $debugbarStart, ['kind' => 'result']);

                        if ($usesEloquentResult) {
                            $models = $this->getWithoutCache($columns);
                            $payload = $this->resultPayloadFromEloquentModels($models);
                        } else {
                            $payload = $this->buildResultPayloadFromQuery($base);
                            $models = $this->hydrateResultPayload($payload, $model, false);
                        }

                        return [$models, $payload];
                    },
                    onStore: function ($payload, $result) {
                        NormCache::storeResultCache(
                            $result->key,
                            $payload,
                            $result->buildingKey,
                            $this->queryTtl,
                            $result->wakeKey,
                            $result->versionKeys,
                            $result->expectedVersions,
                            $result->buildingToken
                        );
                    },
                    onHit: function ($result) use ($model, $debugbarStart) {
                        CacheReporter::queryHit($model, $result->key, $debugbarStart, [
                            'kind' => 'result',
                            'contains' => class_basename($model) . ' (' . count($result->payload) . ' models)',
                        ]);

                        return $this->finalizeResult(NormCache::hydrateResult($result->payload, $this->model));
                    },
                    onBuild: function () use ($columns, $model, $debugbarStart) {
                        CacheReporter::queryMiss($model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'result']);

                        return $this->getWithoutCache($columns);
                    },
                ),
                fn() => $this->getWithoutCache($columns)
            );
        }

        $bypassReasons = $plan->bypassReasons;

        try {
            $result = $this->getDependencyOnlyBypassResult($base, $model, $resolvedCols, $plan);
        } catch (\Throwable $e) {
            NormCache::fallback($e);

            $result = null;
        }

        if ($result !== null) {
            return $result;
        }

        CacheReporter::queryBypassed($model, $bypassReasons, $debugbarStart);

        return $this->getWithoutCache($columns);
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($total !== null || $this->skipCache || !NormCache::isEnabled()) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $base = $this->toBase();
        $plan = $this->cachePlan($base, CachePlanContext::paginationCount($this->inferAggregateDependencies()));

        if ($plan->mode === CacheMode::Bypass) {
            CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons, $debugbarStart);

            return parent::paginate($perPage, $columns, $pageName, $page);
        }

        try {
            $cachedTotal = $this->rememberPaginationTotal($base, $plan, $debugbarStart);
        } catch (\Throwable $e) {
            NormCache::fallback($e);

            $cachedTotal = null;
        }

        return parent::paginate($perPage, $columns, $pageName, $page, $cachedTotal);
    }

    public function eagerLoadRelations(array $models): array
    {
        if ($this->skipCache) {
            foreach ($this->eagerLoad as $name => $constraint) {
                $this->eagerLoad[$name] = function ($query) use ($constraint) {
                    $constraint($query);
                    $builder = $query instanceof EloquentRelation ? $query->getQuery() : $query;
                    if ($builder instanceof self) {
                        $builder->withoutCache();
                    }
                };
            }
        }

        return parent::eagerLoadRelations($models);
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

    public function hydrateResultPayload(array $payload, string $model, bool $cached): Collection
    {
        return $this->finalizeResult(NormCache::hydrateResult($payload, $this->model, $cached));
    }

    public function finalizeResult(array $models): Collection
    {
        if ($models && $this->eagerLoad) {
            $models = $this->eagerLoadRelations($models);
        }

        return $this->applyAfterQueryCallbacks($this->model->newCollection($models));
    }

    public function getWithoutCache($columns): Collection
    {
        return parent::get($columns);
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

    public function cachePlan(QueryBuilder $base, CachePlanContext $context): CachePlan
    {
        return $this->planner()->plan($this, $base, $context);
    }

    private function planner(): CachePlanner
    {
        return self::$sharedPlanner ??= new CachePlanner;
    }

    private function getByQuery(QueryBuilder $base, string $model, ?array $selectedCols, CachePlan $plan, mixed $debugbarStart): Collection
    {
        $hash = QueryHasher::forNormalizedQuery($this);
        $depClasses = $plan->dependencies->depClassesFor($model);
        $depTableKeys = $plan->dependencies->tables;

        return NormCache::executor()->runNormalized(
            fetch: fn() => NormCache::getModelsFromQuery($model, $hash, $this->cacheTag, $depClasses, $depTableKeys),
            waitForBuild: fn() => NormCache::waitForBuild('query_ids', $model, $hash, $this->cacheTag, $depClasses, $depTableKeys),
            onBuild: function () use ($base, $model, $selectedCols, $debugbarStart) {
                CacheReporter::queryMiss($model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $this->finalizeResult(NormCache::getModels($this->buildIds($base), $model, $selectedCols, null, $this, true, $this->model));
            },
            onMiss: function ($result) use ($base, $model, $selectedCols, $debugbarStart) {
                CacheReporter::queryMiss($model, $result->key, $debugbarStart, ['kind' => 'ids']);

                $ids = $this->resolveIds($result->key, $base, $result->buildingKey, $result->versionKeys, $result->expectedVersions, $result->buildingToken);

                return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this, true, $this->model));
            },
            onHit: function ($result) use ($model, $hash, $selectedCols, $debugbarStart) {
                $key = $result->status === CacheStatus::Stale ? "stale:{$hash}" : $result->key;

                CacheReporter::queryHit($model, $key, $debugbarStart, [
                    'kind' => 'ids + models',
                    'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result->ids) . ' ids)',
                    'contains_model' => $result->ids,
                ]);

                return $this->finalizeResult(NormCache::getModels($result->ids, $model, $selectedCols, $result->models, $this, true, $this->model));
            },
        );
    }

    private function getFromCacheableQuery(QueryBuilder $base, string $model, ?array $selectedCols, CachePlan $plan, mixed $debugbarStart): Collection
    {
        if ($plan->primaryKeys !== null
            && $plan->dependencies->depClassesFor($model) === []
            && $plan->dependencies->tables === []) {
            return $this->getModelsByIds($plan->primaryKeys, $model, $selectedCols);
        }

        return $this->getByQuery($base, $model, $selectedCols, $plan, $debugbarStart);
    }

    private function getDependencyOnlyBypassResult(QueryBuilder $base, string $model, ?array $selectedCols, CachePlan $plan): ?Collection
    {
        if (!$this->hasOnlyDependencyBypass($plan->bypassReasons)) {
            return null;
        }

        return $plan->primaryKeys !== null ? $this->getModelsByIds($plan->primaryKeys, $model, $selectedCols) : null;
    }

    private function getModelsByIds(array $ids, string $model, ?array $selectedCols): Collection
    {
        return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this, false, $this->model));
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

        return (int) NormCache::executor()->runScalar(
            fetch: fn() => NormCache::getResultCache($model, $depClasses, $hash, $this->cacheTag, $depTableKeys, CacheKeyBuilder::K_COUNT),
            waitForBuild: fn() => NormCache::waitForBuild('result', $model, $hash, tag: $this->cacheTag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: CacheKeyBuilder::K_COUNT),
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

    /** @param array<string, list<string>> $bypassReasons */
    private function hasOnlyDependencyBypass(array $bypassReasons): bool
    {
        return count($bypassReasons) === 1 && isset($bypassReasons['dependency']);
    }
}
