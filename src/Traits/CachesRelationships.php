<?php

namespace NormCache\Traits;

use Illuminate\Contracts\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;
use NormCache\Facades\NormCache;
use NormCache\Relations\CacheableBelongsTo;
use NormCache\Relations\CacheableBelongsToMany;
use NormCache\Relations\CacheableHasManyThrough;
use NormCache\Relations\CacheableHasOneThrough;
use NormCache\Relations\CacheableMorphToMany;

trait CachesRelationships
{
    protected function newBelongsTo(Builder $query, Model $child, $foreignKey, $ownerKey, $relation)
    {
        if (!NormCache::isEnabled()) {
            return parent::newBelongsTo($query, $child, $foreignKey, $ownerKey, $relation);
        }

        return new CacheableBelongsTo($query, $child, $foreignKey, $ownerKey, $relation);
    }

    protected function newMorphToMany(
        EloquentBuilder $query,
        Model $parent,
        $name,
        $table,
        $foreignPivotKey,
        $relatedPivotKey,
        $parentKey,
        $relatedKey,
        $relationName = null,
        $inverse = false,
    ) {
        if (!NormCache::isEnabled()) {
            return parent::newMorphToMany($query, $parent, $name, $table, $foreignPivotKey, $relatedPivotKey, $parentKey, $relatedKey, $relationName, $inverse);
        }

        return new CacheableMorphToMany(
            $query, $parent, $name, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName, $inverse,
        );
    }

    protected function newHasManyThrough(
        EloquentBuilder $query,
        Model $farParent,
        Model $throughParent,
        $firstKey,
        $secondKey,
        $localKey,
        $secondLocalKey,
    ) {
        if (!NormCache::isEnabled()) {
            return parent::newHasManyThrough($query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey);
        }

        return new CacheableHasManyThrough(
            $query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey,
        );
    }

    protected function newHasOneThrough(
        EloquentBuilder $query,
        Model $farParent,
        Model $throughParent,
        $firstKey,
        $secondKey,
        $localKey,
        $secondLocalKey,
    ) {
        if (!NormCache::isEnabled()) {
            return parent::newHasOneThrough($query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey);
        }

        return new CacheableHasOneThrough(
            $query, $farParent, $throughParent, $firstKey, $secondKey, $localKey, $secondLocalKey,
        );
    }

    protected function newBelongsToMany(
        EloquentBuilder $query,
        Model $parent,
        $table,
        $foreignPivotKey,
        $relatedPivotKey,
        $parentKey,
        $relatedKey,
        $relationName = null,
    ) {
        if (!NormCache::isEnabled()) {
            return parent::newBelongsToMany($query, $parent, $table, $foreignPivotKey, $relatedPivotKey, $parentKey, $relatedKey, $relationName);
        }

        return new CacheableBelongsToMany(
            $query, $parent, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName,
        );
    }
}
