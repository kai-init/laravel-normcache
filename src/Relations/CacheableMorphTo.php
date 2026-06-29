<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\RelationCacheGuards;
use NormCache\Values\CachePlanContext;

class CacheableMorphTo extends MorphTo
{
    public function getEager(): EloquentCollection
    {
        $base = $this->query->toBase();
        $columns = ProjectionClassifier::resolve($base, null);

        foreach ($this->dictionary as $type => $_) {
            $instance = $this->cacheableInstanceForType($type);
            $results = $instance !== null
                ? $this->getResultsFromCache($type, $instance, $columns)
                : $this->getResultsByType($type);

            $this->matchToMorphParents($type, $results);
        }

        return $this->models;
    }

    protected function getResultsByType($type)
    {
        if (!empty($this->macroBuffer)) {
            $instance = $this->createModelByType($type);
            $ownerKey = $this->ownerKey ?? $instance->getKeyName();
            $query = $this->replayMacros($instance->newQuery());

            if ($query instanceof CacheableBuilder) {
                $query->withoutCache();
            }

            $relationQuery = $this->getQuery();
            $query = $query->mergeConstraintsFrom($relationQuery)
                ->with(array_merge(
                    $relationQuery->getEagerLoads(),
                    (array) ($this->morphableEagerLoads[get_class($instance)] ?? [])
                ))
                ->withCount(
                    (array) ($this->morphableEagerLoadCounts[get_class($instance)] ?? [])
                );

            if ($callback = ($this->morphableConstraints[get_class($instance)] ?? null)) {
                $callback($query);
            }

            $whereIn = $this->whereInMethod($instance, $ownerKey);

            return $query->{$whereIn}(
                $instance->qualifyColumn($ownerKey), $this->gatherKeysByType($type, $instance->getKeyType())
            )->get();
        }

        return parent::getResultsByType($type);
    }

    private function cacheableInstanceForType(string $type): ?Model
    {
        $class = Model::getActualClassNameForMorph($type);

        if (isset($this->morphableConstraints[$class]) || isset($this->morphableEagerLoadCounts[$class])) {
            return null;
        }

        $instance = $this->createModelByType($type);
        $query = $instance->newQuery();

        if (!empty($this->macroBuffer)) {
            $query = $this->replayMacros($query);
        }

        if ($this->ownerKey !== null && $this->ownerKey !== $instance->getKeyName()) {
            return null;
        }

        if (!$query instanceof CacheableBuilder) {
            return null;
        }

        $prepared = $query->prepareCacheExecution();
        $builder = $prepared->builder;
        $base = $prepared->base;

        if ($this->isSimpleMorphBase($base, $builder)) {
            return $instance;
        }

        $plan = $builder->cachePlan($base, CachePlanContext::morphToEagerLoad($type));

        return $plan->isNormalized() ? $instance : null;
    }

    private function isSimpleMorphBase(QueryBuilder $base, CacheableBuilder $builder): bool
    {
        // At this point only global scopes (e.g. soft-delete) have been applied — no FK constraints yet.
        // Accept only an optional soft-delete Null where; reject everything else.
        if (RelationCacheGuards::blocksBypass($builder, $base)
            || RelationCacheGuards::hasOrderingOrJoins($base)) {
            return false;
        }

        $whereCount = count($base->wheres);

        if ($whereCount > 1) {
            return false;
        }

        if ($whereCount === 1 && ($base->wheres[0]['type'] ?? null) !== 'Null') {
            return false;
        }

        return true;
    }

    private function getResultsFromCache(string $type, Model $instance, ?array $columns): EloquentCollection
    {
        $class = $instance::class;
        $ids = array_values($this->gatherKeysByType($type, $instance->getKeyType()));

        $missedQuery = $this->query;
        if (!empty($this->macroBuffer)) {
            $queryWithMacros = $this->replayMacros($instance->newQuery());
            if ($queryWithMacros instanceof CacheableBuilder) {
                $queryWithMacros->withoutCache();
            }
            $missedQuery = $queryWithMacros->mergeConstraintsFrom($this->query);
        }

        $models = NormCache::withSpaceForModel(
            $class,
            null,
            fn() => NormCache::getModels($ids, $class, $columns, null, $missedQuery, false),
        );
        $collection = $instance->newCollection($models);

        $eagerLoads = $this->morphableEagerLoads[$class] ?? [];

        if (!empty($eagerLoads)) {
            $collection->load($eagerLoads);
        }

        return $collection;
    }
}
