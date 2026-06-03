<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlanContext;
use NormCache\Planning\QueryAnalyzer;

class CacheableMorphTo extends MorphTo
{
    public function getEager(): EloquentCollection
    {
        foreach (array_keys($this->dictionary) as $type) {
            $results = $this->shouldUseCacheForType($type)
                ? $this->getResultsFromCache($type)
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

    private function shouldUseCacheForType(string $type): bool
    {
        if (!empty($this->macroBuffer)) {
            return false;
        }

        $class = Model::getActualClassNameForMorph($type);

        if (isset($this->morphableConstraints[$class]) || isset($this->morphableEagerLoadCounts[$class])) {
            return false;
        }

        $instance = $this->createModelByType($type);
        $query = $instance->newQuery();

        if ($this->ownerKey !== null && $this->ownerKey !== $instance->getKeyName()) {
            return false;
        }

        if (!$query instanceof CacheableBuilder) {
            return false;
        }

        $plan = $query->cachePlan($query->toBase(), CachePlanContext::morphToEagerLoad($type));

        return $plan->mode === CacheMode::Normalized;
    }

    private function getResultsFromCache(string $type): EloquentCollection
    {
        $class = Model::getActualClassNameForMorph($type);
        $instance = $this->createModelByType($type);
        $ids = array_values($this->gatherKeysByType($type, $instance->getKeyType()));

        $columns = QueryAnalyzer::resolveSelectedColumns($this->query->toBase(), null);

        $models = NormCache::getModels($ids, $class, $columns, null, $this->query, false);
        $collection = $instance->newCollection($models);

        $eagerLoads = $this->morphableEagerLoads[$class] ?? [];

        if (!empty($eagerLoads)) {
            $collection->load($eagerLoads);
        }

        return $collection;
    }
}
