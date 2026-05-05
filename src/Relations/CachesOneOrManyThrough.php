<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!NormCache::isEnabled()) {
            return parent::get($columns);
        }

        $builder = $this->prepareQueryBuilder($columns);
        $base = $builder->toBase();

        $key = 'through:v' . NormCache::currentVersion($this->related::class)
            . ':v' . NormCache::currentVersion($this->throughParent::class)
            . ':' . QueryHasher::hash($base);

        $cached = NormCache::get($key);

        if ($cached !== null) {
            $prototype = new ($this->related::class);

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
