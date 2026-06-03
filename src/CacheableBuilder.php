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
use NormCache\Facades\NormCache;
use NormCache\Planning\BypassReasons;
use NormCache\Planning\CachePlan;
use NormCache\Planning\CachePlanContext;
use NormCache\Planning\CachePlanner;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Relations\CachesRelationAggregates;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Traits\Cacheable;
use NormCache\Traits\CachesScalarResults;
use NormCache\Traits\HandlesBuilderInvalidation;

class CacheableBuilder extends Builder
{
    use CachesRelationAggregates, CachesScalarResults, HandlesBuilderInvalidation;

    private static array $validatedModelClasses = [];

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private ?array $dependsOn = null;

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

    public function remember(int $ttl): static
    {
        if ($ttl <= 0) {
            throw new \InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->queryTtl = $ttl;

        return $this;
    }

    public function ttl(int $ttl): static
    {
        return $this->remember($ttl);
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

    public function explicitDependencies(): ?array
    {
        return $this->dependsOn;
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
        $resolvedCols = QueryAnalyzer::resolveSelectedColumns($base, ['*']);
        $plan = $this->cachePlan($base, CachePlanContext::models($resolvedCols, $this->inferAggregateDependencies()));

        if ($plan->mode === CacheMode::Normalized) {
            return $plan->primaryKeys !== null ? 'cached: direct (primary key)' : 'cached';
        }

        if ($plan->mode === CacheMode::Result) {
            return $this->dependsOn !== null ? 'cached: result (dependsOn())' : 'cached: result';
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
        $resolvedCols = QueryAnalyzer::resolveSelectedColumns($base, (array) $columns);
        $model = $this->model::class;
        $plan = $this->cachePlan($base, CachePlanContext::models($resolvedCols, $this->inferAggregateDependencies()));

        if ($plan->mode === CacheMode::Normalized) {
            return NormCache::rescue(
                fn() => $this->getFromCacheableQuery($base, $model, $resolvedCols, $plan),
                fn() => $this->getWithoutCache($columns)
            );
        }

        if ($plan->mode === CacheMode::Result) {
            return NormCache::rescue(
                fn() => (new VersionedCache)->rememberCollection(
                    $this,
                    $base,
                    $plan,
                    $columns,
                    $this->queryTtl,
                    $this->cacheTag
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
            $cachedTotal = $this->rememberPaginationTotal($base, $plan);
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

    public function cursor(): LazyCollection
    {
        return parent::cursor();
    }

    public function lazy($chunkSize = 1000): LazyCollection
    {
        return parent::lazy($chunkSize);
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
        return $this->finalizeResult(NormCache::hydrateResult($payload, $model, $cached));
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
        return (new CachePlanner)->plan($this, $base, $context);
    }

    public function prepareResultCacheQuery(QueryBuilder $base): void
    {
        if (!empty($base->joins) && empty($base->columns)) {
            if (config('app.debug')) {
                logger()->warning('NormCache: dependsOn() JOIN without explicit select — added ' . $this->model->getTable() . '.* automatically.');
            }

            $base->select($this->model->getTable() . '.*');
        }
    }

    private function getByQuery(QueryBuilder $base, string $model, ?array $selectedCols, CachePlan $plan): Collection
    {
        $debugbarStart = CacheReporter::beginMeasure();

        $hash = QueryHasher::forNormalizedQuery($base);

        $depClasses = $plan->dependencies->depClassesFor($model);
        $depTableKeys = $plan->dependencies->tables;

        // 1. Resolve Query -> IDs
        $result = NormCache::getModelsFromQuery($model, $hash, $this->cacheTag, $depClasses, $depTableKeys);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild('query_ids', $model, $hash, $this->cacheTag, $depClasses, $depTableKeys);

            if ($result === null) {
                CacheReporter::queryMiss($model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $this->finalizeResult(NormCache::getModels($this->buildIds($base), $model, $selectedCols, null, $this));
            }
        }

        // 2. Fetch/Store IDs if miss
        if ($result['status'] === 'miss') {
            CacheReporter::queryMiss($model, $result['key'], $debugbarStart, ['kind' => 'ids']);

            $ids = $this->resolveIds($result['key'], $base, $result['buildingKey'], $result['versionKeys'], $result['expectedVersions'], $result['buildingToken'] ?? null);

            return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this));
        }

        $key = $result['status'] === 'stale' ? "stale:{$hash}" : $result['key'];

        CacheReporter::queryHit($model, $key, $debugbarStart, [
            'kind' => 'ids + models',
            'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result['ids']) . ' ids)',
            'contains_model' => $result['ids'],
        ]);

        // 3. Resolve IDs -> Models (normalized path)
        return $this->finalizeResult(NormCache::getModels($result['ids'], $model, $selectedCols, $result['models'], $this));
    }

    private function getFromCacheableQuery(QueryBuilder $base, string $model, ?array $selectedCols, CachePlan $plan): Collection
    {
        if ($plan->primaryKeys !== null
            && $plan->dependencies->depClassesFor($model) === []
            && $plan->dependencies->tables === []) {
            return $this->getModelsByIds($plan->primaryKeys, $model, $selectedCols);
        }

        return $this->getByQuery($base, $model, $selectedCols, $plan);
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
        return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this, false));
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

    private function rememberPaginationTotal(QueryBuilder $base, CachePlan $plan): int
    {
        return (new VersionedCache)->rememberPaginationCount(
            $this,
            $base,
            $plan,
            $this->queryTtl,
            $this->cacheTag
        );
    }

    /** @param array<string, list<string>> $bypassReasons */
    private function hasOnlyDependencyBypass(array $bypassReasons): bool
    {
        return count($bypassReasons) === 1 && isset($bypassReasons['dependency']);
    }
}
