<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Support\ProjectionClassifier;
use NormCache\Values\CachePlanContext;

class CacheableMorphTo extends MorphTo
{
    public function getEager(): EloquentCollection
    {
        $columns = ProjectionClassifier::resolve($this->query->toBase(), null);

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

            $query = $query->mergeConstraintsFrom($this->getQuery())
                ->with(array_merge(
                    $this->getQuery()->getEagerLoads(),
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
        if (!empty($this->macroBuffer)) {
            return null;
        }

        $class = Model::getActualClassNameForMorph($type);

        if (isset($this->morphableConstraints[$class]) || isset($this->morphableEagerLoadCounts[$class])) {
            return null;
        }

        $instance = $this->createModelByType($type);
        $query = $instance->newQuery();

        if ($this->ownerKey !== null && $this->ownerKey !== $instance->getKeyName()) {
            return null;
        }

        if (!$query instanceof CacheableBuilder) {
            return null;
        }

        $plan = $query->cachePlan($query->toBase(), CachePlanContext::morphToEagerLoad($type));

        return $plan->mode === CacheMode::Normalized ? $instance : null;
    }

    private function getResultsFromCache(string $type, Model $instance, ?array $columns): EloquentCollection
    {
        $class = $instance::class;
        $ids = array_values($this->gatherKeysByType($type, $instance->getKeyType()));

        $models = NormCache::getModels($ids, $class, $columns, null, $this->query, false);
        $collection = $instance->newCollection($models);

        $eagerLoads = $this->morphableEagerLoads[$class] ?? [];

        if (!empty($eagerLoads)) {
            $collection->load($eagerLoads);
        }

        return $collection;
    }
}
