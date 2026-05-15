<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;

/**
 * @mixin Model
 */
trait Cacheable
{
    use CachesRelationships;

    protected bool $withoutCacheNext = false;

    public static function bootCacheable(): void
    {
        if (!config('normcache.enabled', true)) {
            return;
        }

        static::created(fn($model) => NormCache::invalidateVersion($model));
        static::updated(fn($model) => $model->flush());
        static::deleted(fn($model) => $model->flush());
        static::registerModelEvent('restored', fn($model) => NormCache::invalidateVersion($model));
    }

    public function flush(): void
    {
        NormCache::flushInstance($this);
    }

    public function newEloquentBuilder($query)
    {
        if (!config('normcache.enabled', true)) {
            return parent::newEloquentBuilder($query);
        }

        $builder = new CacheableBuilder($query);

        if ($this->withoutCacheNext) {
            $this->withoutCacheNext = false;

            $builder->withoutCache();
        }

        return $builder;
    }

    public function refresh(): static
    {
        return $this->runWithoutCache(fn() => parent::refresh());
    }

    public function fresh($with = []): ?static
    {
        return $this->runWithoutCache(fn() => parent::fresh($with));
    }

    private function runWithoutCache(callable $callback)
    {
        $previous = $this->withoutCacheNext;
        $this->withoutCacheNext = true;

        try {
            return $callback();
        } finally {
            $this->withoutCacheNext = $previous;
        }
    }
}
