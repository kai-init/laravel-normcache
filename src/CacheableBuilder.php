<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Arr;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Support\QueryInspector;
use NormCache\Traits\Cacheable;
use NormCache\Traits\CachesScalarResults;
use NormCache\Traits\HandlesCacheInvalidation;

class CacheableBuilder extends Builder
{
    use CachesScalarResults, HandlesCacheInvalidation;

    private static array $validatedModelClasses = [];

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private ?array $dependsOn = null;

    private bool $cacheAggregates = true;

    private array $pendingAggregates = [];

    private ?string $cacheTag = null;

    // -------------------------------------------------------------------------
    // Fluent configuration
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
        $this->queryTtl = $ttl;

        return $this;
    }

    public function withoutAggregateCache(): static
    {
        $this->cacheAggregates = false;

        return $this;
    }

    public function tag(string $tag): static
    {
        $this->cacheTag = $tag;

        return $this;
    }

    public function dependsOn(array $modelClasses): static
    {
        if (empty($modelClasses)) {
            throw new \InvalidArgumentException('dependsOn() requires at least one model class.');
        }

        foreach ($modelClasses as $class) {
            if (!is_string($class) || (!isset(self::$validatedModelClasses[$class]) && !is_a($class, Model::class, true))) {
                throw new \InvalidArgumentException("dependsOn() class '{$class}' is not an Eloquent model.");
            }
            self::$validatedModelClasses[$class] = true;
        }

        $this->dependsOn = $modelClasses;

        return $this;
    }

    // -------------------------------------------------------------------------
    // Public overrides
    // -------------------------------------------------------------------------

    public function withAggregate($relations, $function, $column = '*'): static
    {
        if (!$this->cacheAggregates) {
            return parent::withAggregate($relations, $function, $column);
        }

        $uncacheable = [];

        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
                $constraint = null;
            }

            $relatedClass = $this->model->{$name}()->getRelated()::class;

            if (!self::relatedIsCacheable($relatedClass)) {
                if ($constraint !== null) {
                    $uncacheable[$name] = $constraint;
                } else {
                    $uncacheable[] = $name;
                }

                continue;
            }

            $this->pendingAggregates[] = [
                'name' => $name,
                'constraint' => $constraint,
                'function' => strtolower($function),
                'column' => $column,
            ];
        }

        if (!empty($uncacheable)) {
            parent::withAggregate($uncacheable, $function, $column);
        }

        return $this;
    }

    public function explain(): string
    {
        $base = $this->toBase();
        $resolvedCols = QueryInspector::resolveSelectedColumns($base, ['*']);
        $grouped = $this->computeBypassReasons($base, $resolvedCols);

        if (empty($grouped)) {
            return $this->dependsOn !== null ? 'cached: dependsOn() opt-in' : 'cached';
        }

        if ($this->dependsOn !== null && !isset($grouped['safety']) && !isset($grouped['opted_out'])) {
            return 'cached: raw (dependsOn())';
        }

        $labels = QueryInspector::categoryLabels();
        $parts = [];
        foreach ($grouped as $category => $reasons) {
            $parts[] = ($labels[$category] ?? $category) . ': ' . implode(', ', $reasons);
        }

        return 'not cached — ' . implode(' | ', $parts);
    }

    public function get($columns = ['*']): Collection
    {
        if ($this->skipCache || !NormCache::isEnabled()) {
            return $this->getWithoutCache($columns);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $base = $this->toBase();
        $resolvedCols = QueryInspector::resolveSelectedColumns($base, (array) $columns);
        $model = $this->model::class;

        try {
            if ($this->shouldUseCache($base, $resolvedCols)) {
                return $this->getFromCacheableQuery($base, $model, $resolvedCols);
            }

            if ($this->shouldUseRawCache($base)) {
                return $this->getFromRawCache($base, $model, $this->queryCacheKey($base), $this->cacheTag);
            }

            $bypassReasons = $this->computeBypassReasons($base, $resolvedCols);
            $result = $this->getDependencyOnlyBypassResult($base, $model, $resolvedCols, $bypassReasons);

            if ($result !== null) {
                return $result;
            }

            $this->recordBypass($model, $bypassReasons, $debugbarStart);

            return $this->getWithoutCache($columns);
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return $this->getWithoutCache($columns);
        }
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($total !== null || $this->skipCache || !NormCache::isEnabled()) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $base = $this->toBase();
        // Count caching does not depend on selected row columns.

        if ($this->shouldUseCache($base)) {
            return $this->paginateWithCountCache($base, 'count', $perPage, $columns, $pageName, $page, $total);
        }

        if ($this->shouldUseRawCache($base)) {
            return $this->paginateWithCountCache($base, 'raw count', $perPage, $columns, $pageName, $page, $total);
        }

        $bypassReasons = $this->computeBypassReasons($base);

        $this->recordBypass($this->model::class, $bypassReasons, $debugbarStart);

        return parent::paginate($perPage, $columns, $pageName, $page, $total);
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

    // -------------------------------------------------------------------------
    // Private — query execution
    // -------------------------------------------------------------------------

    private function paginateWithCountCache(QueryBuilder $base, string $kind, $perPage, $columns, string $pageName, $page, $fallbackTotal): LengthAwarePaginator
    {
        try {
            return parent::paginate($perPage, $columns, $pageName, $page, $this->resolvePaginationTotal($base, $kind));
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return parent::paginate($perPage, $columns, $pageName, $page, $fallbackTotal);
        }
    }

    private function resolvePaginationTotal(QueryBuilder $base, string $kind): int
    {
        $queryStart = NormCacheCollector::beginMeasure();

        ['key' => $countKey, 'data' => $data] = NormCache::getNamespacedCache(
            'count',
            $this->model::class,
            $this->queryCacheKey($base),
            $this->dependsOn ?? [],
            $this->cacheTag
        );

        $cachedTotal = $data[0] ?? null;

        if (NormCache::isEventsEnabled()) {
            event($cachedTotal !== null
                ? new QueryCacheHit($this->model::class, $countKey)
                : new QueryCacheMiss($this->model::class, $countKey)
            );
        }

        NormCacheCollector::recordQuery(
            $cachedTotal !== null ? 'query hit' : 'query miss',
            $this->model::class,
            $countKey,
            $queryStart,
            ['kind' => $kind]
        );

        if ($cachedTotal === null) {
            $cachedTotal = $base->getCountForPagination();
            NormCache::storeQueryAggregate($countKey, $cachedTotal, $this->queryTtl);
        }

        return (int) $cachedTotal;
    }

    private function getByQuery(QueryBuilder $base, string $model, ?array $selectedCols): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();

        $hash = $this->queryCacheKey($base);

        if ($this->dependsOn !== null) {
            return $this->getFromRawCache($base, $model, $hash, $this->cacheTag);
        }

        $result = NormCache::getModelsFromQuery($model, $hash, $this->cacheTag);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild($model, $hash, tag: $this->cacheTag);

            if ($result === null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheMiss($model, 'building:budget-exhausted'));
                }

                NormCacheCollector::recordQuery('query miss', $model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $this->finalizeResult(NormCache::getModels($this->buildIds($base), $model, $selectedCols, null, $this));
            }
        }

        if ($result['status'] === 'miss') {
            NormCacheCollector::recordQuery('query miss', $model, $result['key'], $debugbarStart, ['kind' => 'ids']);

            $ids = $this->resolveIds($result['key'], $base, $result['buildingKey'], $result['versionKeys'], $result['expectedVersions']);

            return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this));
        }

        $key = $result['status'] === 'stale' ? "stale:{$hash}" : $result['key'];

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $key));
        }

        NormCacheCollector::recordQuery('query hit', $model, $key, $debugbarStart, [
            'kind' => 'ids + models',
            'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result['ids']) . ' ids)',
            'contains_model' => $result['ids'],
        ]);

        return $this->finalizeResult(NormCache::getModels($result['ids'], $model, $selectedCols, $result['models'], $this));
    }

    private function getFromRawCache(QueryBuilder $base, string $model, string $hash, ?string $tag = null): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();

        $result = NormCache::getRawCache($model, $this->dependsOn, $hash, $tag);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild($model, $hash, returnOnMiss: false, depClasses: $this->dependsOn, tag: $tag);

            if ($result === null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheMiss($model, 'building:budget-exhausted'));
                }

                NormCacheCollector::recordQuery('query miss', $model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'deps']);

                $blob = array_map(fn($r) => (array) $r, $base->get()->all());

                return $this->finalizeResult(NormCache::hydrateRaw($blob, $model, false));
            }
        }

        if ($result['status'] === 'miss') {
            if (NormCache::isEventsEnabled()) {
                event(new QueryCacheMiss($model, $result['key']));
            }

            NormCacheCollector::recordQuery('query miss', $model, $result['key'], $debugbarStart, ['kind' => 'deps']);

            $blob = array_map(fn($r) => (array) $r, $base->get()->all());
            NormCache::storeRawResult($result['key'], $blob, $result['buildingKey'], $this->queryTtl);

            return $this->finalizeResult(NormCache::hydrateRaw($blob, $model, false));
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $result['key']));
        }

        NormCacheCollector::recordQuery('query hit', $model, $result['key'], $debugbarStart, [
            'kind' => 'raw',
            'contains' => class_basename($model) . ' (' . count($result['blob']) . ' models)',
        ]);

        return $this->finalizeResult(NormCache::hydrateRaw($result['blob'], $model));
    }

    private function getFromCacheableQuery(QueryBuilder $base, string $model, ?array $selectedCols): Collection
    {
        $ids = $this->extractPrimaryKeys($base);

        if ($ids !== null) {
            return $this->getModelsByIds($ids, $model, $selectedCols);
        }

        return $this->getByQuery($base, $model, $selectedCols);
    }

    /** @param array<string, list<string>> $bypassReasons */
    private function getDependencyOnlyBypassResult(QueryBuilder $base, string $model, ?array $selectedCols, array $bypassReasons): ?Collection
    {
        if (!$this->hasOnlyDependencyBypass($bypassReasons)) {
            return null;
        }

        $ids = $this->extractPrimaryKeys($base);

        return $ids === null ? null : $this->getModelsByIds($ids, $model, $selectedCols);
    }

    /** @param array<int, mixed> $ids */
    private function getModelsByIds(array $ids, string $model, ?array $selectedCols): Collection
    {
        return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this, false));
    }

    private function resolveIds(string $key, QueryBuilder $base, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = []): array
    {
        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheMiss($this->model::class, $key));
        }

        $ids = $this->buildIds($base);
        NormCache::storeQueryIds($key, $ids, $this->queryTtl, $buildingKey, $versionKeys, $expectedVersions);

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

    private function finalizeResult(array $models): Collection
    {
        if (!empty($this->pendingAggregates)) {
            $models = (new RelationAggregateLoader($this->model))->load($models, $this->pendingAggregates);
        }

        if ($models && $this->eagerLoad) {
            $models = $this->eagerLoadRelations($models);
        }

        return $this->applyAfterQueryCallbacks($this->model->newCollection($models));
    }

    // -------------------------------------------------------------------------
    // Private — fallback path
    // -------------------------------------------------------------------------

    private function getWithoutCache($columns): Collection
    {
        $this->replayPendingAggregates();

        return parent::get($columns);
    }

    private function replayPendingAggregates(): void
    {
        foreach ($this->pendingAggregates as $agg) {
            $relations = $agg['constraint'] !== null
                ? [$agg['name'] => $agg['constraint']]
                : $agg['name'];

            parent::withAggregate($relations, $agg['function'], $agg['column']);
        }
    }

    // -------------------------------------------------------------------------
    // Private — guards and key helpers
    // -------------------------------------------------------------------------

    /** @return array<string, list<string>> */
    private function computeBypassReasons(QueryBuilder $base, ?array $resolvedColumns = null): array
    {
        if ($this->skipCache) {
            return ['opted_out' => ['withoutCache() was called explicitly']];
        }

        if (!NormCache::isEnabled()) {
            return ['opted_out' => ['cache is globally disabled']];
        }

        if ($this->insideTransaction()) {
            return ['safety' => ['inside a database transaction']];
        }

        $reasons = QueryInspector::bypassReasons($base, $this->model->getTable(), $resolvedColumns);

        if ($this->dependsOn !== null) {
            unset($reasons['dependency']);
        }

        return $reasons;
    }

    private function shouldUseCache(QueryBuilder $base, ?array $resolvedColumns = null): bool
    {
        return !$this->insideTransaction()
            && QueryInspector::isStructurallyCacheable($base, $this->model->getTable(), $resolvedColumns)
            && ($this->dependsOn !== null || !QueryInspector::hasDependencyBypass($base));
    }

    private function shouldUseRawCache(QueryBuilder $base): bool
    {
        return $this->dependsOn !== null
            && !$this->insideTransaction()
            && !QueryInspector::hasSafetyBypass($base);
    }

    /** @param array<string, list<string>> $bypassReasons */
    private function hasOnlyDependencyBypass(array $bypassReasons): bool
    {
        return count($bypassReasons) === 1 && isset($bypassReasons['dependency']);
    }

    private function extractPrimaryKeys(QueryBuilder $base): ?array
    {
        return QueryInspector::extractPrimaryKeys($base, $this->model->getKeyName(), $this->model->getQualifiedKeyName());
    }

    private function recordBypass(string $modelClass, array $bypassReasons, ?float $debugbarStart): void
    {
        if (NormCache::isEventsEnabled()) {
            event(new QueryBypassed($modelClass, $bypassReasons));
        }

        NormCacheCollector::recordBypass($modelClass, $bypassReasons, $debugbarStart);
    }

    private static function relatedIsCacheable(string $class): bool
    {
        static $cache = [];

        return $cache[$class] ??= in_array(Cacheable::class, class_uses_recursive($class), true);
    }

    private function insideTransaction(): bool
    {
        return $this->model->getConnection()->transactionLevel() > 0;
    }

    private function queryCacheKey(QueryBuilder $base): string
    {
        $cols = $base->columns;
        $base->columns = null;
        try {
            return QueryHasher::fromQuery($base);
        } finally {
            $base->columns = $cols;
        }
    }
}
