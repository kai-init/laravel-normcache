<?php

namespace NormCache\Relations;

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
        $base = $builder->toBase();
        $hash = QueryHasher::hash($base);

        $cacheData = NormCache::getThroughCache($this->related::class, $this->throughParent::class, $hash);
        $key = $cacheData['key'];
        $cached = $cacheData['data'];

        if ($cached !== null) {
            $models = NormCache::getModels($cached, $this->related::class);

            if ($models && $builder->getEagerLoads()) {
                $models = $builder->eagerLoadRelations($models);
            }

            return $this->query->applyAfterQueryCallbacks(
                $this->related->newCollection($models)
            );
        }

        $result = parent::get($columns);

        $relatedClass = $this->related::class;
        $ids = array_map(fn($m) => $m->getKey(), $result->all());
        NormCache::set($key, $ids, NormCache::queryTtl());

        // Populate model cache while we have full model
        if ($columns === ['*']) {
            $attrsByKey = [];
            foreach ($result as $model) {
                $attrsByKey[NormCache::modelKey($relatedClass, $model->getKey())] = $model->getAttributes();
            }
            if (!empty($attrsByKey)) {
                NormCache::setManyModels($relatedClass, $attrsByKey, NormCache::ttl());
            }
        }

        return $result;
    }
}
