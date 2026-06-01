<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Support\Arr;
use NormCache\CacheableBuilder;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

/** @mixin BelongsToMany */
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
        $columns = Arr::wrap($columns);
        $cacheParentIds = $this->getCacheParentIds();

        if (!$this->shouldUsePivotCache($cacheParentIds)) {
            return parent::get($columns);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $parentClass = $this->parent::class;
        $relatedClass = $this->related::class;
        $parentClassKey = NormCache::classKey($parentClass);
        $constraintHash = $this->currentConstraintHash($columns);
        $shouldCacheRelatedModels = $this->shouldCacheRelatedModels($columns);
        $selectedRelatedColumns = $this->selectedRelatedColumns($columns);

        try {
            $cache = NormCache::getPivotCache(
                $parentClass,
                $relatedClass,
                $this->relationName,
                $cacheParentIds,
                $constraintHash
            );

            $seg = $cache['seg'];
            $cachedByParentId = $cache['data'];
            $missedIds = array_keys(array_filter($cachedByParentId, fn($v) => !is_array($v)));

            if (empty($missedIds)) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheHit($parentClass, "pivot:{$parentClassKey}:{$this->relationName}"));
                }
                NormCacheCollector::recordQuery(
                    'pivot hit',
                    $parentClass,
                    "pivot:{$parentClassKey}:{$this->relationName}",
                    $debugbarStart,
                    ['parents' => $cacheParentIds, 'related' => $relatedClass]
                );

                return $this->hydrateFromPivotCache($cachedByParentId, $relatedClass, $selectedRelatedColumns);
            }

            if (NormCache::isEventsEnabled()) {
                event(new QueryCacheMiss($parentClass, "pivot:{$parentClassKey}:{$this->relationName}"));
            }
            NormCacheCollector::recordQuery(
                'pivot miss',
                $parentClass,
                "pivot:{$parentClassKey}:{$this->relationName}",
                $debugbarStart,
                ['parents' => $cacheParentIds, 'related' => $relatedClass]
            );

            $results = parent::get($columns);

            $relatedKey = NormCache::classKey($relatedClass);
            $keyMap = [];
            foreach ($cacheParentIds as $parentId) {
                $keyMap[$parentId] = "pivot:{{$parentClassKey}}:{$relatedKey}:{$this->relationName}:{$constraintHash}:{$seg}:{$parentId}";
            }

            $this->populatePivotCache(
                $results,
                $keyMap,
                $relatedClass,
                $shouldCacheRelatedModels,
                $cache['versionKeys'],
                $cache['expectedVersions']
            );

            return $results;
        } catch (\Exception $e) {
            NormCache::Throwable($e);

            return parent::get($columns);
        }
    }

    private function currentConstraintHash(array $columns): string
    {
        $base = $this->query->toBase();
        $qualifiedKey = $this->getQualifiedForeignPivotKeyName();

        $shape = [];

        if ($columns !== ['*']) {
            $shape['columns'] = $columns;
        }

        $wheres = [];
        foreach ($base->wheres as $where) {
            if (($where['column'] ?? null) !== $qualifiedKey) {
                $wheres[] = $where;
            }
        }

        if ($wheres !== []) {
            $shape['wheres'] = $wheres;
        }

        foreach ([
            'orders',
            'limit',
            'offset',
            'groups',
            'havings',
            'joins',
            'distinct',
            'unions',
            'unionOrders',
            'unionLimit',
            'unionOffset',
            'lock',
        ] as $property) {
            if ($base->{$property} !== null && $base->{$property} !== []) {
                $shape[$property] = $base->{$property};
            }
        }

        // WHERE bindings intentionally excluded — they contain the FK constraint's parent IDs,
        // which must not vary the per-query constraint hash.
        $rawBindings = $base->getRawBindings();
        foreach (['order', 'having', 'join'] as $group) {
            if (!empty($rawBindings[$group])) {
                $shape['bindings_' . $group] = $rawBindings[$group];
            }
        }

        if (empty($shape)) {
            return 'nc';
        }

        return QueryHasher::hash(json_encode($shape));
    }

    private function shouldUsePivotCache(array $cacheParentIds): bool
    {
        if (!NormCache::isEnabled()
            || empty($cacheParentIds)
            || !$this->query instanceof CacheableBuilder
            || $this->query->isCacheSkipped()
            || $this->query->hasRemovedScopes()
            || $this->parent->getConnection()->transactionLevel() !== 0
            || $this->query->toBase()->lock !== null) {
            return false;
        }

        // whereRaw with ? params can't be separated from FK bindings — same SQL, different values would collide.
        foreach ($this->query->toBase()->wheres as $where) {
            if (($where['type'] ?? null) === 'raw' && str_contains(($where['sql'] ?? ''), '?')) {
                return false;
            }
        }

        return true;
    }

    private function getCacheParentIds(): array
    {
        if ($this->inEagerLoad) {
            return $this->eagerParentIds;
        }

        if ($this->parent->exists && $this->parent->getKey() !== null) {
            return [$this->parent->getKey()];
        }

        return [];
    }

    private function shouldCacheRelatedModels(array $columns): bool
    {
        return $columns === ['*'] && $this->query->toBase()->columns === null;
    }

    private function selectedRelatedColumns(array $columns): ?array
    {
        $queryColumns = $this->query->toBase()->columns;

        if ($queryColumns !== null) {
            return $queryColumns;
        }

        return $columns === ['*'] ? null : $columns;
    }

    private function populatePivotCache(Collection $results, array $keyMap, string $relatedClass, bool $cacheRelatedModels, array $versionKeys, array $expectedVersions): void
    {
        $pivotMap = array_fill_keys(array_keys($keyMap), []);
        $modelAttrs = [];
        $pivotPrefix = $this->accessor . '_';

        foreach ($results as $model) {
            $pivotModel = $model->getRelation($this->accessor);
            $parentId = $pivotModel->getAttribute($this->foreignPivotKey);

            if (array_key_exists($parentId, $pivotMap)) {
                $pivotMap[$parentId][] = [
                    'id' => $model->getKey(),
                    'pivot' => $pivotModel->getRawOriginal(),
                ];
            }

            if ($cacheRelatedModels) {
                $modelAttrs[$model->getKey()] = array_filter(
                    $model->getRawOriginal(),
                    fn($k) => !str_starts_with($k, $pivotPrefix),
                    ARRAY_FILTER_USE_KEY
                );
            }
        }

        $pivotEntriesByKey = [];
        foreach ($pivotMap as $parentId => $entries) {
            $pivotEntriesByKey[$keyMap[$parentId]] = $entries;
        }

        foreach ($pivotEntriesByKey as $key => $payload) {
            if (!NormCache::storeVersionedResult($key, $payload, versionKeys: $versionKeys, expectedVersions: $expectedVersions)) {
                return;
            }
        }

        NormCache::cacheModelAttrs($relatedClass, $modelAttrs);
    }

    private function hydrateFromPivotCache(array $cachedByParentId, string $relatedClass, ?array $selectedRelatedColumns): Collection
    {
        $allRelatedIds = [];
        foreach ($cachedByParentId as $entries) {
            foreach ($entries as $entry) {
                $allRelatedIds[] = $entry['id'];
            }
        }

        $modelsById = [];
        foreach (NormCache::getModels(array_unique($allRelatedIds), $relatedClass, $selectedRelatedColumns) as $model) {
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

        if ($result && $this->query->getEagerLoads()) {
            $result = $this->query->eagerLoadRelations($result);
        }

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection($result)
        );
    }
}
