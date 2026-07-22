<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Planning\QueryEligibility;

final class RelationCacheGuards
{
    // Conditions shared by every relation's "simple shape" bypass check, regardless of relation type.
    public static function blocksBypass(CacheableBuilder $builder, QueryBuilder $base): bool
    {
        return QueryEligibility::blocksSimpleRelation(
            $builder,
            $base,
            (bool) config('normcache.enabled', true),
        );
    }

    // Shared by belongsTo/morphTo eager loads, which require an unmodified base query shape.
    public static function hasOrderingOrJoins(QueryBuilder $base): bool
    {
        return QueryEligibility::hasOrderingOrJoins($base);
    }
}
