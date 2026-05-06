<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;
use NormCache\Relations\CacheableBelongsToMany;
use NormCache\Relations\CacheableHasManyThrough;
use NormCache\Relations\CacheableHasOneThrough;
use NormCache\Relations\CacheableMorphToMany;

/**
 * @mixin Model
 */
trait Cacheable
{
    public static function bootCacheable(): void
    {
        if (!config('normcache.enabled', true)) {
            return;
        }

        static::created(fn($model) => NormCache::invalidateVersion($model::class, $model->getConnectionName()));
        static::updated(fn($model) => $model->flush());
        static::deleting(fn($model) => $model->flush());
        static::registerModelEvent('restored', fn($model) => NormCache::invalidateVersion($model::class, $model->getConnectionName()));
    }

    public function flush(): void
    {
        NormCache::flushInstance(static::class, $this->getKey(), $this->getConnectionName());
    }

    public function newEloquentBuilder($query)
    {
        if (!config('normcache.enabled', true)) {
            return parent::newEloquentBuilder($query);
        }
        return new CacheableBuilder($query);
    }

    protected function newMorphToMany(
        \Illuminate\Contracts\Database\Eloquent\Builder $query,
        \Illuminate\Database\Eloquent\Model $parent,
        $name,
        $table,
        $foreignPivotKey,
        $relatedPivotKey,
        $parentKey,
        $relatedKey,
        $relationName = null,
        $inverse = false,
    ) {
        if (!config('normcache.enabled', true)) {
            return parent::newMorphToMany($query, $parent, $name, $table, $foreignPivotKey, $relatedPivotKey, $parentKey, $relatedKey, $relationName, $inverse);
        }
        return new CacheableMorphToMany(
            $query, $parent, $name, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName, $inverse,
        );
    }

    protected function newHasManyThrough(
        \Illuminate\Contracts\Database\Eloquent\Builder $query,
        \Illuminate\Database\Eloquent\Model $farParent,
        \Illuminate\Database\Eloquent\Model $throughParent,
        $firstKey,
        $secondKey,
        $localKey,
        $secondLocalKey,
    ) {
        if (!config('normcache.enabled', true)) {
            return parent::newHasManyThrough($query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey);
        }
        return new CacheableHasManyThrough(
            $query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey,
        );
    }

    protected function newHasOneThrough(
        \Illuminate\Contracts\Database\Eloquent\Builder $query,
        \Illuminate\Database\Eloquent\Model $farParent,
        \Illuminate\Database\Eloquent\Model $throughParent,
        $firstKey,
        $secondKey,
        $localKey,
        $secondLocalKey,
    ) {
        if (!config('normcache.enabled', true)) {
            return parent::newHasOneThrough($query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey);
        }
        return new CacheableHasOneThrough(
            $query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey,
        );
    }

    protected function newBelongsToMany(
        \Illuminate\Contracts\Database\Eloquent\Builder $query,
        \Illuminate\Database\Eloquent\Model $parent,
        $table,
        $foreignPivotKey,
        $relatedPivotKey,
        $parentKey,
        $relatedKey,
        $relationName = null,
    ) {
        if (!config('normcache.enabled', true)) {
            return parent::newBelongsToMany($query, $parent, $table, $foreignPivotKey, $relatedPivotKey, $parentKey, $relatedKey, $relationName);
        }
        return new CacheableBelongsToMany(
            $query, $parent, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName,
        );
    }
}
