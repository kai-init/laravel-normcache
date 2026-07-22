<?php

namespace NormCache\Planning;

use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\SpaceValidationResult;

final class QueryEligibility
{
    public static function isCacheAvailable(CacheableBuilder $builder, bool $cacheEnabled = true): bool
    {
        return !$builder->isCacheSkipped() && $cacheEnabled;
    }

    public static function usesWritePdo(QueryBuilder $query): bool
    {
        return $query->useWritePdo;
    }

    public static function isInsideTransaction(CacheableBuilder $builder): bool
    {
        return $builder->getModel()->getConnection()->transactionLevel() > 0;
    }

    public static function hasExplicitLock(QueryBuilder $query): bool
    {
        return $query->lock !== null && $query->lock !== false;
    }

    public static function hasGroupedShape(QueryBuilder $query): bool
    {
        return !empty($query->groups) || !empty($query->havings);
    }

    public static function hasUnion(QueryBuilder $query): bool
    {
        return !empty($query->unions);
    }

    public static function blocksSimpleRelation(
        CacheableBuilder $builder,
        QueryBuilder $query,
        bool $cacheEnabled = true,
    ): bool {
        return !self::isCacheAvailable($builder, $cacheEnabled)
            || self::usesWritePdo($query)
            || self::isInsideTransaction($builder)
            || self::hasGroupedShape($query)
            || self::hasUnion($query)
            || self::hasExplicitLock($query)
            || $builder->hasExplicitDependencies();
    }

    public static function hasOrderingOrJoins(QueryBuilder $query): bool
    {
        return !empty($query->joins)
            || !empty($query->orders)
            || $query->limit !== null
            || $query->offset > 0
            || $query->distinct;
    }

    public function qualifiesForDirectModels(
        bool $explain,
        bool $insideTransaction,
        bool $hasExplicitDependencies,
        QueryInspection $inspection,
    ): bool {
        return !$explain
            && !$insideTransaction
            && !$hasExplicitDependencies
            && $inspection->contextReasons === []
            && $inspection->dependencies->safe
            && $inspection->dependencies->hasNoDependencies()
            && $inspection->primaryKeys !== null
            && $inspection->normalizationFlags() === 0
            && !$inspection->hasSafetyBypass();
    }

    public function dependsOnPrimaryModelOnly(
        bool $hasExplicitDependencies,
        QueryInspection $inspection,
    ): bool {
        return !$hasExplicitDependencies
            && $inspection->dependencies->safe
            && $inspection->dependencies->hasNoDependencies()
            && !$inspection->hasDependencyBypass()
            && !isset($inspection->contextReasons['dependency']);
    }

    public function canUseModelIndex(
        QueryInspection $inspection,
        DependencySet $dependencies,
    ): bool {
        return $inspection->dependencies->hasNoDependencies()
            && !$inspection->hasDependencyBypass()
            && !isset($inspection->contextReasons['dependency'])
            && $inspection->normalizationFlags() === 0
            && !isset($inspection->contextReasons['normalization'])
            && $dependencies->safe;
    }

    public function canUseResult(
        QueryInspection $inspection,
        DependencySet $dependencies,
        bool $hasExplicitDependencies,
    ): bool {
        return $dependencies->safe
            && ($hasExplicitDependencies || !$inspection->dependencies->hasNoDependencies());
    }

    public function canUseSimpleResult(
        QueryInspection $inspection,
        int $blockerFlags,
    ): bool {
        return $inspection->dependencies->safe
            && $inspection->dependencies->hasNoDependencies()
            && ($inspection->flags & $blockerFlags) === 0;
    }

    public function hasSafetyBypass(QueryInspection $inspection, bool $insideTransaction): bool
    {
        return $insideTransaction || $inspection->hasSafetyBypass();
    }

    public function safetyReasons(QueryInspection $inspection, bool $insideTransaction): array
    {
        return BypassReasons::merge(
            $insideTransaction ? ['safety' => ['inside a database transaction']] : [],
            BypassReasons::fromInspection($inspection),
        )['safety'] ?? [];
    }

    public function requiresExplicitSelectForJoinResult(
        CacheableBuilder $builder,
        QueryBuilder $query,
        CachePlanContext $context,
    ): bool {
        return $context->selectAll
            && !empty($query->joins)
            && empty($query->columns)
            && !$builder->hasAggregateColumns();
    }

    public function isCanonicalModelQuery(QueryInspection $inspection): bool
    {
        return $inspection->normalizationFlags() === 0
            && !isset($inspection->contextReasons['normalization']);
    }

    public function fitsCacheSpace(SpaceValidationResult $validation): bool
    {
        return $validation->isValid;
    }
}
