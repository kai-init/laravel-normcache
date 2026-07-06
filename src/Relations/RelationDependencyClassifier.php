<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Database\Eloquent\Relations\Relation;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Traits\Cacheable;
use NormCache\Values\CachePlanContext;
use NormCache\Values\RelationDependency;

final class RelationDependencyClassifier
{
    /**
     * @param  Relation<*, *, *>  $relation
     */
    public function classify(Relation $relation, ?callable $constraint): ?RelationDependency
    {
        $relatedClass = $relation->getRelated()::class;

        if (!self::relatedIsCacheable($relatedClass)) {
            return null;
        }

        $relationExtraTables = [];
        $relationQuery = $relation->getQuery();

        if ($relationQuery instanceof CacheableBuilder) {
            if ($relationQuery->isCacheSkipped()) {
                return null;
            }

            $relationBase = $relationQuery->toBase();
            $inspection = (new QueryAnalyzer)->inspect($relationBase, $relation->getRelated()->getTable(), null);

            if ($inspection->hasDependencyBypass() || $inspection->hasSafetyBypass()) {
                return null;
            }

            $joinDeps = (new QueryAnalyzer)->inferJoinDependencies(
                $relationBase,
                $relationQuery->getModel()->getConnection()->getName()
            );

            if (!$joinDeps->safe || (!empty($relationBase->joins) && $joinDeps->tables === [])) {
                return null;
            }

            $relationExtraTables = $joinDeps->tables;
        }

        $constraintModels = [];
        $constraintTables = [];

        if ($constraint !== null) {
            try {
                $testBuilder = ($relatedClass)::query();
                $constraint($testBuilder);

                if (!$testBuilder instanceof CacheableBuilder) {
                    return null;
                }

                $prepared = $testBuilder->prepareCacheExecution();
                $plan = $prepared->builder->cachePlan($prepared->base, CachePlanContext::models());

                if (!$plan->isCacheable()) {
                    return null;
                }

                $constraintModels = $plan->dependencies->models;
                $constraintTables = $plan->dependencies->tables;
            } catch (\Throwable) {
                return null;
            }
        }

        $throughClass = null;
        if ($relation instanceof HasOneOrManyThrough) {
            $through = (new \ReflectionProperty($relation, 'throughParent'))->getValue($relation)::class;
            if (self::relatedIsCacheable($through)) {
                $throughClass = $through;
            }
        }

        $tableKey = null;
        if ($relation instanceof BelongsToMany) {
            $tableKey = NormCache::tableKey(
                $relation->getParent()->getConnection()->getName(),
                $relation->getTable(),
            );
        }

        return new RelationDependency(
            relatedClass: $relatedClass,
            throughClass: $throughClass,
            tableKey: $tableKey,
            constraintModels: $constraintModels,
            constraintTables: array_values(array_unique([...$constraintTables, ...$relationExtraTables])),
        );
    }

    private static function relatedIsCacheable(string $class): bool
    {
        static $cache = [];

        return $cache[$class] ??= in_array(Cacheable::class, class_uses_recursive($class), true);
    }
}
