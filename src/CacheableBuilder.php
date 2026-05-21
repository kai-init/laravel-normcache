<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Arr;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Support\QueryInspector;
use NormCache\Traits\HandlesCacheInvalidation;

class CacheableBuilder extends Builder
{
    use HandlesCacheInvalidation;

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

    public function get($columns = ['*']): Collection
    {
        $base = $this->toBase();

        if (!$this->shouldCache($base)) {
            return $this->getWithoutCache($columns);
        }

        $model = $this->model::class;
        $selectedCols = QueryInspector::resolveSelectedColumns($base, $columns);

        if (QueryInspector::hasCalculatedColumns($selectedCols)) {
            return $this->getWithoutCache($columns);
        }

        try {
            $ids = QueryInspector::extractPrimaryKeys($base, $this->model->getKeyName(), $this->model->getQualifiedKeyName());

            if ($ids !== null) {
                return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this, false));
            }

            return $this->getByQuery($base, $model, $selectedCols);
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return $this->getWithoutCache($columns);
        }
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        $base = $this->toBase();

        if ($total !== null || !$this->shouldCache($base)) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $hash = $this->queryCacheKey($base);

        try {
            $cacheData = NormCache::getNamespacedCache('count', $this->model::class, $hash);
            $countKey = $cacheData['key'];
            $cachedTotal = $cacheData['data'];

            if ($cachedTotal === null) {
                $cachedTotal = $base->getCountForPagination();
                NormCache::storeCount($countKey, $cachedTotal, $this->queryTtl);
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
        $hash = $this->queryCacheKey($base);
        $cacheData = NormCache::getModelsFromQuery($model, $hash);
        $key = $cacheData['key'];

        if ($cacheData['ids'] === null) {
            return $this->finalizeResult(NormCache::getModels(
                $this->resolveIds($key, $base, $cacheData['lock']),
                $model, $selectedCols, null, $this
            ));
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $key));
        }

        return $this->finalizeResult(NormCache::getModels($cacheData['ids'], $model, $selectedCols, $cacheData['models'], $this));
    }

    private function resolveIds(string $key, QueryBuilder $base, ?string $lockKey = null): array
    {
        if ($lockKey === null) {
            return NormCache::pollQueryIds($key) ?? $this->buildIds($base);
        }

        // We own the lock (acquired in Lua eval).
        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheMiss($this->model::class, $key));
        }

        try {
            $ids = $this->buildIds($base);
            NormCache::storeQueryIds($key, $ids, $lockKey, $this->queryTtl);
            $lockKey = null;
        } finally {
            if ($lockKey) {
                NormCache::releaseLock($lockKey);
            }
        }

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
            $models = (new AggregateLoader($this->model))->load($models, $this->pendingAggregates);
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

    private function shouldCache(QueryBuilder $base): bool
    {
        return !$this->skipCache
            && NormCache::isEnabled()
            && !$this->insideTransaction()
            && QueryInspector::isPureModelQuery($base, $this->model->getTable());
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
