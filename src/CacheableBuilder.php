<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Arr;
use NormCache\Traits\CachesScalarResults;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Support\QueryInspector;
use NormCache\Traits\HandlesCacheInvalidation;

class CacheableBuilder extends Builder
{
    use CachesScalarResults, HandlesCacheInvalidation;

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private bool $cacheAggregates = true;

    private array $pendingAggregates = [];

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

    // -------------------------------------------------------------------------
    // Public overrides
    // -------------------------------------------------------------------------

    public function withAggregate($relations, $function, $column = '*'): static
    {
        if (!$this->cacheAggregates) {
            return parent::withAggregate($relations, $function, $column);
        }

        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
                $constraint = null;
            }

            $this->pendingAggregates[] = [
                'name' => $name,
                'constraint' => $constraint,
                'function' => strtolower($function),
                'column' => $column,
            ];
        }

        return $this;
    }

    public function explain(): string
    {
        $base = $this->toBase();
        $resolvedCols = QueryInspector::resolveSelectedColumns($base, ['*']);
        $grouped = $this->computeBypassReasons($base, $resolvedCols);

        if (empty($grouped)) {
            return 'cached';
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
        $bypassReasons = $this->computeBypassReasons($base, $resolvedCols);

        if (!empty($bypassReasons)) {
            if (NormCache::isEventsEnabled()) {
                event(new QueryBypassed($this->model::class, $bypassReasons));
            }

            NormCacheCollector::recordBypass($this->model::class, $bypassReasons, $debugbarStart);

            return $this->getWithoutCache($columns);
        }

        $model = $this->model::class;

        try {
            $ids = QueryInspector::extractPrimaryKeys($base, $this->model->getKeyName(), $this->model->getQualifiedKeyName());

            if ($ids !== null) {
                return $this->finalizeResult(NormCache::getModels($ids, $model, $resolvedCols, null, $this, false));
            }

            return $this->getByQuery($base, $model, $resolvedCols);
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
        // resolvedColumns omitted — paginate caches the count, not rows; column selection is irrelevant.
        $bypassReasons = $this->computeBypassReasons($base);

        if (!empty($bypassReasons)) {
            if (NormCache::isEventsEnabled()) {
                event(new QueryBypassed($this->model::class, $bypassReasons));
            }

            NormCacheCollector::recordBypass($this->model::class, $bypassReasons, $debugbarStart);

            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $hash = $this->queryCacheKey($base);

        $queryStart = NormCacheCollector::beginMeasure();

        try {
            $countKey = NormCache::getNamespacedCache('count', $this->model::class, $hash)['key'];
            $cachedTotal = NormCache::getQueryAggregate($countKey);

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
                ['kind' => 'count']
            );

            if ($cachedTotal === null) {
                $cachedTotal = $base->getCountForPagination();
                NormCache::storeQueryAggregate($countKey, $cachedTotal, $this->queryTtl);
            }

            return parent::paginate($perPage, $columns, $pageName, $page, (int) $cachedTotal);
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }
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

    private function getByQuery(QueryBuilder $base, string $model, ?array $selectedCols): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();

        $hash = $this->queryCacheKey($base);
        $cacheData = NormCache::getModelsFromQuery($model, $hash);
        $key = $cacheData['key'];

        if ($cacheData['ids'] === null) {
            NormCacheCollector::recordQuery('query miss', $model, $key, $debugbarStart, ['kind' => 'ids']);

            $ids = $this->resolveIds($key, $base);

            return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this));
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $key));
        }

        NormCacheCollector::recordQuery('query hit', $model, $key, $debugbarStart, [
            'kind' => 'ids + models',
            'contains' => 'model hit: ' . class_basename($model) . ' (' . count($cacheData['ids']) . ' ids)',
            'contains_model' => $cacheData['ids'],
        ]);

        return $this->finalizeResult(NormCache::getModels($cacheData['ids'], $model, $selectedCols, $cacheData['models'], $this));
    }

    private function resolveIds(string $key, QueryBuilder $base): array
    {
        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheMiss($this->model::class, $key));
        }

        $ids = $this->buildIds($base);
        NormCache::storeQueryIds($key, $ids, $this->queryTtl);

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

        return $this->model->newCollection($models);
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
        if ($this->skipCache)           { return ['opted_out' => ['withoutCache() was called explicitly']]; }
        if (!NormCache::isEnabled())    { return ['opted_out' => ['cache is globally disabled']]; }
        if ($this->insideTransaction()) { return ['safety'    => ['inside a database transaction']]; }

        return QueryInspector::bypassReasons($base, $this->model->getTable(), $resolvedColumns);
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
