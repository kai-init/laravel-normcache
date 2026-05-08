<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!NormCache::isEnabled() || $this->parent->getConnection()->transactionLevel() > 0) {
            return parent::get($columns);
        }

        $builder = $this->prepareQueryBuilder($columns);
        $hash = QueryHasher::hash($builder->toBase());
        $relatedClass = $this->related::class;

        $cacheData = NormCache::getThroughCache($relatedClass, $this->throughParent::class, $hash);
        $key = $cacheData['key'];
        $lockKey = $cacheData['lock'];

        if ($cacheData['data'] !== null) {
            return $this->hydrateFromIds($cacheData['data'], $relatedClass, $builder);
        }

        if ($lockKey === null) {
            $ids = $this->pollForThroughCache($key);
            if ($ids !== null) {
                return $this->hydrateFromIds($ids, $relatedClass, $builder);
            }
        }

        try {
            $result = parent::get($columns);
            $ids = array_map(fn($m) => $m->getKey(), $result->all());

            if ($lockKey !== null) {
                NormCache::setAndReleaseLock($key, $ids, NormCache::queryTtl(), $lockKey);
                $lockKey = null;
            } else {
                NormCache::set($key, $ids, NormCache::queryTtl());
            }
        } finally {
            if ($lockKey !== null) {
                NormCache::delete($lockKey);
            }
        }

        // Populate model cache while we have full model
        if ($columns === ['*']) {
            $attrsByKey = [];
            foreach ($result as $model) {
                $attrsByKey[NormCache::modelKey($relatedClass, $model->getKey())] = $model->getRawOriginal();
            }
            if (!empty($attrsByKey)) {
                NormCache::setManyModels($relatedClass, $attrsByKey, NormCache::ttl());
            }
        }

        return $result;
    }

    private function hydrateFromIds(array $ids, string $relatedClass, Builder $builder): Collection
    {
        $models = NormCache::getModels($ids, $relatedClass);

        if ($models && $builder->getEagerLoads()) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection($models)
        );
    }

    private function pollForThroughCache(string $key): ?array
    {
        // Exponential backoff: 20ms → 40ms → 80ms → 160ms → 200ms (500ms total).
        $delay = 20_000;
        for ($i = 0; $i < 5; $i++) {
            usleep($delay);
            $ids = NormCache::get($key);
            if ($ids !== null) {
                return $ids;
            }
            $delay = min($delay * 2, 200_000);
        }
        return null;
    }
}
