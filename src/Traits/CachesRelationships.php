<?php

namespace NormCache\Traits;

use Illuminate\Contracts\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use NormCache\Relations\CacheableBelongsToMany;
use NormCache\Relations\CacheableHasManyThrough;
use NormCache\Relations\CacheableHasOneThrough;
use NormCache\Relations\CacheableMorphToMany;

trait CachesRelationships
{
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
        if (!config('normcache.enabled', true)) {
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
        if (!config('normcache.enabled', true)) {
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
        if (!config('normcache.enabled', true)) {
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
        if (!config('normcache.enabled', true)) {
            return parent::newBelongsToMany($query, $parent, $table, $foreignPivotKey, $relatedPivotKey, $parentKey, $relatedKey, $relationName);
        }
        return new CacheableBelongsToMany(
            $query, $parent, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName,
        );
    }
}
