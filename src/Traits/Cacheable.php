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

        return new CacheableBuilder($query);
    }
}
