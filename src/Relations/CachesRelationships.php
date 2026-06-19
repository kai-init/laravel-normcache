<?php

namespace NormCache\Relations;

use Illuminate\Contracts\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

trait CachesRelationships
{
    protected function newBelongsTo(Builder $query, Model $child, $foreignKey, $ownerKey, $relation)
    {
        return new CacheableBelongsTo($query, $child, $foreignKey, $ownerKey, $relation);
    }

    protected function newHasOne(Builder $query, Model $parent, $foreignKey, $localKey)
    {
        return new CacheableHasOne($query, $parent, $foreignKey, $localKey);
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
        return new CacheableBelongsToMany(
            $query, $parent, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey, $relatedKey, $relationName,
        );
    }

    protected function newMorphTo(
        Builder $query,
        Model $parent,
        $foreignKey,
        $ownerKey,
        $type,
        $relation,
    ) {
        return new CacheableMorphTo($query, $parent, $foreignKey, $ownerKey, $type, $relation);
    }
}
