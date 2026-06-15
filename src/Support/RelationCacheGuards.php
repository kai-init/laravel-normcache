<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;

final class RelationCacheGuards
{
    /**
     * Conditions shared by every relation's "simple shape" bypass check:
     * cache disabled/opted-out, inside a transaction, or any query feature
     * that the planner can't normalize regardless of relation type.
     */
    public static function blocksBypass(CacheableBuilder $builder, QueryBuilder $base): bool
    {
        return $builder->isCacheSkipped()
            || !NormCache::isEnabled()
            || $builder->getModel()->getConnection()->transactionLevel() > 0
            || !empty($base->groups)
            || !empty($base->havings)
            || !empty($base->unions)
            || ($base->lock !== null && $base->lock !== false)
            || $builder->hasExplicitDependencies();
    }

    /** Shared by belongsTo/morphTo eager loads, which require an unmodified base query shape. */
    public static function hasOrderingOrJoins(QueryBuilder $base): bool
    {
        return !empty($base->joins)
            || !empty($base->orders)
            || $base->limit !== null
            || $base->offset > 0
            || $base->distinct;
    }
}
