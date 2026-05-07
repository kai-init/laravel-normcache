<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use NormCache\CacheableBuilder;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;

/** @mixin \Illuminate\Database\Eloquent\Relations\BelongsToMany */
trait CachesPivotRelation
{
    private array $eagerParentIds = [];
    private bool $inEagerLoad = false;

    public function addEagerConstraints(array $models): void
    {
        $this->inEagerLoad = true;
        $this->eagerParentIds = $this->getKeys($models, $this->parentKey);
        parent::addEagerConstraints($models);
    }

    public function get($columns = ['*']): Collection
    {
        if (!$this->shouldUsePivotCache()) {
            return parent::get($columns);
        }

        $parentClass = $this->parent::class;
        $relatedClass = $this->related::class;
        $parentClassKey = NormCache::classKey($parentClass);

        $cache = NormCache::getPivotCache(
            $parentClass,
            $relatedClass,
            $this->relationName,
            $this->eagerParentIds
        );

        $parentVersion = $cache['parentVersion'];
        $relatedVersion = $cache['relatedVersion'];
        $cachedByParentId = $cache['data'];

        $missedIds = array_keys(array_filter($cachedByParentId, fn($v) => !is_array($v)));

        if (!empty($missedIds)) {
            event(new QueryCacheMiss($parentClass, "pivot:{$parentClassKey}:{$this->relationName}"));
            
            $results = parent::get($columns);
            
            $relatedKey = NormCache::classKey($relatedClass);
            $keyMap = [];
            foreach ($this->eagerParentIds as $parentId) {
                $keyMap[$parentId] = "pivot:{{$parentClassKey}}:{$relatedKey}:{$this->relationName}:v{$parentVersion}:v{$relatedVersion}:{$parentId}";
            }
            
            $this->populatePivotCache($results, $keyMap, $relatedClass);

            return $results;
        }

        event(new QueryCacheHit($parentClass, "pivot:{$parentClassKey}:{$this->relationName}"));

        return $this->hydrateFromPivotCache($cachedByParentId, $relatedClass);
    }

    private function shouldUsePivotCache(): bool
    {
        return $this->inEagerLoad
            && NormCache::isEnabled()
            && !empty($this->eagerParentIds)
            && $this->query instanceof CacheableBuilder
            && !$this->query->isCacheSkipped()
            && $this->parent->getConnection()->transactionLevel() === 0;
    }

    private function populatePivotCache(Collection $results, array $keyMap, string $relatedClass): void
    {
        $pivotMap = array_fill_keys(array_keys($keyMap), []);
        $toModelCache = [];

        foreach ($results as $model) {
            $pivotModel = $model->getRelation($this->accessor);
            $parentId = $pivotModel->getAttribute($this->foreignPivotKey);

            if (array_key_exists($parentId, $pivotMap)) {
                $pivotMap[$parentId][] = [
                    'id' => $model->getKey(),
                    'pivot' => $pivotModel->getAttributes(),
                ];
            }

            $toModelCache[NormCache::modelKey($relatedClass, $model->getKey())] = $model->getAttributes();
        }

        $toPivotCache = [];
        foreach ($pivotMap as $parentId => $entries) {
            $toPivotCache[$keyMap[$parentId]] = $entries;
        }
        NormCache::setMany($toPivotCache, NormCache::queryTtl());

        if (!empty($toModelCache)) {
            NormCache::setManyModels($relatedClass, $toModelCache, NormCache::ttl());
        }
    }

    private function hydrateFromPivotCache(array $cachedByParentId, string $relatedClass): Collection
    {
        $allRelatedIds = [];
        foreach ($cachedByParentId as $entries) {
            foreach ($entries as $entry) {
                $allRelatedIds[] = $entry['id'];
            }
        }

        $modelsById = [];
        foreach (NormCache::getModels(array_unique($allRelatedIds), $relatedClass) as $model) {
            $modelsById[$model->getKey()] = $model;
        }

        $result = [];
        foreach ($cachedByParentId as $entries) {
            foreach ($entries as $entry) {
                if (!isset($modelsById[$entry['id']])) {
                    continue;
                }
                $model = clone $modelsById[$entry['id']];
                $model->setRelation($this->accessor, $this->newExistingPivot($entry['pivot']));
                $result[] = $model;
            }
        }

        return $this->related->newCollection($result);
    }
}
