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

        $cacheData = NormCache::getNamespacedCache('query', $model, $hash);
        $key = $cacheData['key'];

        if ($cacheData['data'] !== null) {
            $ids = $cacheData['data'];
            event(new QueryCacheHit($model, $key));
        } else {
            $ids = $this->resolveIds($key, $base);
        }

        $models = NormCache::getModels($ids, $model, $selectedCols);

        if (!empty($this->pendingAggregates)) {
            $models = (new AggregateLoader($this->model))->load($models, $this->pendingAggregates);
        }

        if ($models && $this->eagerLoad) {
            $models = $this->eagerLoadRelations($models);
        }

        return $this->model->newCollection($models);
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

    private function resolveIds(string $key, QueryBuilder $base): array
    {
        $lockKey = 'building:' . $key;

        if (!NormCache::setIfAbsent($lockKey, 1, 5)) {
            usleep(50_000);

            return NormCache::get($key) ?? $this->buildIds($base);
        }

        event(new QueryCacheMiss($this->model::class, $key));

        try {
            $ids = $this->buildIds($base);
            NormCache::setAndReleaseLock($key, $ids, $this->queryTtl ?? NormCache::queryTtl(), $lockKey);
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
