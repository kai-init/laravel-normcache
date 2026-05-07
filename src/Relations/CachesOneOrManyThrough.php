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
            $prototype = $this->related;

            $models = array_map(function ($attrs) use ($prototype) {
                $instance = clone $prototype;
                $instance->exists = true;
                $instance->setRawAttributes($attrs, true);

                return $instance;
            }, $cached);

            if ($models && $builder->getEagerLoads()) {
                $models = $builder->eagerLoadRelations($models);
            }

            return $this->query->applyAfterQueryCallbacks(
                $this->related->newCollection($models)
            );
        }

        $result = parent::get($columns);

        NormCache::set(
            $key,
            array_map(fn($m) => $m->getAttributes(), $result->all()),
            NormCache::queryTtl(),
        );

        return $result;
    }
}
