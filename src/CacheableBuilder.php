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
use NormCache\Traits\HandlesCacheInvalidation;

class CacheableBuilder extends Builder
{
    use HandlesCacheInvalidation;

    protected bool $skipCache = false;
    protected ?int $queryTtl = null;
    protected bool $cacheAggregates = false;
    protected array $pendingAggregates = [];

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

    public function cacheAggregates(): static
    {
        $this->cacheAggregates = true;

        return $this;
    }

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
        if ($this->skipCache || !NormCache::isEnabled() || $this->insideTransaction()) {
            $this->replayPendingAggregates();

            return parent::get($columns);
        }

        $model = $this->model::class;
        $base = $this->toBase();

        if ($ids = $this->extractPrimaryKeyValues($base)) {
            $selectedCols = $this->resolveSelectedColumns($base, $columns);
            if (!$this->hasCalculatedColumns($selectedCols)) {
                $models = NormCache::getModels($ids, $model, $selectedCols);

                return $this->finalizeResult($models);
            }
        }

        if (!$this->isPureModelQuery($base)) {
            $this->replayPendingAggregates();

            return parent::get($columns);
        }

        $selectedCols = $this->resolveSelectedColumns($base, $columns);

        if ($this->hasCalculatedColumns($selectedCols)) {
            $this->replayPendingAggregates();

            return parent::get($columns);
        }

        $keyBase = (clone $base);
        $keyBase->columns = null;
        $hash = $this->queryCacheKey($keyBase);

        $cacheData = NormCache::getModelsFromQuery($model, $hash);
        $key = $cacheData['key'];

        if ($cacheData['ids'] !== null) {
            event(new QueryCacheHit($model, $key));
            $models = NormCache::getModels($cacheData['ids'], $model, $selectedCols, $cacheData['models']);
        } else {
            $ids = $this->resolveIds($key, $base, $cacheData['lock']);
            $models = NormCache::getModels($ids, $model, $selectedCols);
        }

        return $this->finalizeResult($models);
    }

    protected function finalizeResult(array $models): Collection
    {
        if (!empty($this->pendingAggregates)) {
            $models = (new AggregateLoader($this->model))->load($models, $this->pendingAggregates);
        }

        if ($models && $this->eagerLoad) {
            $models = $this->eagerLoadRelations($models);
        }

        return $this->model->newCollection($models);
    }

    protected function extractPrimaryKeyValues(QueryBuilder $base): ?array
    {
        if (!$this->isPureModelQuery($base) 
            || !empty($base->orders) 
            || $base->offset > 0 
            || $base->limit > 0
        ) {
            return null;
        }

        if (count($base->wheres) !== 1) {
            return null;
        }

        $where = $base->wheres[0];
        $pk = $this->model->getKeyName();
        $qualifiedPk = $this->model->getQualifiedKeyName();

        if ($where['column'] !== $pk && $where['column'] !== $qualifiedPk) {
            return null;
        }

        if ($where['type'] === 'Basic' && $where['operator'] === '=') {
            return [$where['value']];
        }

        if ($where['type'] === 'In') {
            return $where['values'];
        }

        return null;
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

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($this->skipCache || !NormCache::isEnabled() || $this->insideTransaction() || $total !== null) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $base = $this->toBase();

        if (!$this->isPureModelQuery($base)) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $hash = QueryHasher::hash($base);

        $cacheData = NormCache::getNamespacedCache('count', $this->model::class, $hash);
        $countKey = $cacheData['key'];
        $cachedTotal = $cacheData['data'];

        if ($cachedTotal === null) {
            $cachedTotal = $base->getCountForPagination();
            NormCache::set($countKey, $cachedTotal, $this->queryTtl ?? NormCache::queryTtl());
        }

        return parent::paginate($perPage, $columns, $pageName, $page, (int) $cachedTotal);
    }

    private function isPureModelQuery(QueryBuilder $base): bool
    {
        foreach ((array) $base->orders as $order) {
            if (isset($order['type']) && $order['type'] === 'Raw') {
                return false;
            }
        }

        return empty($base->joins)
            && empty($base->groups)
            && empty($base->havings)
            && empty($base->unions)
            && empty($base->aggregate)
            && is_null($base->lock);
    }

    private function insideTransaction(): bool
    {
        return $this->model->getConnection()->transactionLevel() > 0;
    }

    private function resolveSelectedColumns(QueryBuilder $base, ?array $fallback): ?array
    {
        $cols = $base->columns ?? (($fallback === null || $fallback === ['*']) ? null : $fallback);

        if ($cols === null || $cols === ['*']) {
            return null;
        }

        foreach ($cols as $c) {
            if ($c instanceof \Illuminate\Database\Query\Expression || !str_ends_with((string) $c, '*')) {
                return $cols;
            }
        }

        return null;
    }

    private function hasCalculatedColumns(?array $cols): bool
    {
        if ($cols === null) {
            return false;
        }

        foreach ($cols as $col) {
            if ($col instanceof \Illuminate\Database\Query\Expression) {
                return true;
            }
        }

        return false;
    }

    private function queryCacheKey(QueryBuilder $base): string
    {
        return QueryHasher::hash($base);
    }

    private function resolveIds(string $key, QueryBuilder $base, ?string $lockKey = null): array
    {
        // Lock is held by another process — poll until it populates the cache.
        // Exponential backoff: 20ms → 40ms → 80ms → 160ms → 200ms (500ms total).
        if ($lockKey === null) {
            $delay = 20_000; // 20ms
            for ($i = 0; $i < 5; $i++) {
                usleep($delay);
                $ids = NormCache::getQueryIds($key);
                if ($ids !== null) {
                    return $ids;
                }
                $delay = min($delay * 2, 200_000); // cap at 200ms
            }

            return $this->buildIds($base);
        }

        // We own the lock (acquired in Lua eval).
        event(new QueryCacheMiss($this->model::class, $key));

        try {
            $ids = $this->buildIds($base);
            NormCache::setQueryResultsAndReleaseLock($key, $ids, $this->queryTtl ?? NormCache::queryTtl(), $lockKey);
            $lockKey = null;
        } finally {
            if ($lockKey) {
                NormCache::delete($lockKey);
            }
        }

        return $ids;
    }

    private function buildIds(QueryBuilder $base): array
    {
        return (clone $base)
            ->select($this->model->getQualifiedKeyName())
            ->pluck($this->model->getKeyName())
            ->all();
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
}
