<?php

namespace NormCache\Traits;

use Illuminate\Contracts\Database\Eloquent\Builder;
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
        $conn = $this->getConnectionName();
        NormCache::deferDelete(NormCache::modelKey(static::class, $this->getKey()), $conn);
        NormCache::invalidateVersion(static::class, $conn);
    }

    public function newEloquentBuilder($query): CacheableBuilder
    {
        return new CacheableBuilder($query);
    }

    protected function newMorphToMany(
        Builder $query,
        Model $parent,
        $name,
        $table,
        $foreignPivotKey,
        $relatedPivotKey,
        $parentKey,
        $relatedKey,
        $relationName = null,
        $inverse = false,
    ): CacheableMorphToMany {
        return new CacheableMorphToMany(
            $query, $parent, $name, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName, $inverse,
        );
    }

    protected function newHasManyThrough(
        Builder $query,
        Model $farParent,
        Model $throughParent,
        $firstKey,
        $secondKey,
        $localKey,
        $secondLocalKey,
    ): CacheableHasManyThrough {
        return new CacheableHasManyThrough(
            $query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey,
        );
    }

    protected function newHasOneThrough(
        Builder $query,
        Model $farParent,
        Model $throughParent,
        $firstKey,
        $secondKey,
        $localKey,
        $secondLocalKey,
    ): CacheableHasOneThrough {
        return new CacheableHasOneThrough(
            $query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey,
        );
    }

    protected function newBelongsToMany(
        Builder $query,
        Model $parent,
        $table,
        $foreignPivotKey,
        $relatedPivotKey,
        $parentKey,
        $relatedKey,
        $relationName = null,
    ): CacheableBelongsToMany {
        return new CacheableBelongsToMany(
            $query, $parent, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName,
        );
    }
}
