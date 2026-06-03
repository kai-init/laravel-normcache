<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Support\Arr;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlanContext;
use NormCache\Support\CacheReporter;
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

        $debugbarStart = CacheReporter::beginMeasure();

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
                $constraintHash,
                $this->pivotTableKey()
            );

            $seg = $cache['seg'];
            $cachedByParentId = $cache['data'];
            $missedIds = array_keys(array_filter($cachedByParentId, fn($v) => !is_array($v)));

            if (empty($missedIds)) {
                CacheReporter::queryHit($parentClass, "pivot:{$parentClassKey}:{$this->relationName}", $debugbarStart, [
                    'parents' => $cacheParentIds,
                    'related' => $relatedClass,
                ], 'pivot hit');

                return $this->hydrateFromPivotCache($cachedByParentId, $relatedClass, $selectedRelatedColumns);
            }

            CacheReporter::queryMiss($parentClass, "pivot:{$parentClassKey}:{$this->relationName}", $debugbarStart, [
                'parents' => $cacheParentIds,
                'related' => $relatedClass,
            ], 'pivot miss');

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
            NormCache::fallback($e);

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

        // Bindings included for order, having, join AND where.
        // Redundant parent IDs in where bindings are safe as the pivot key already differentiates by parent.
        $rawBindings = $base->getRawBindings();
        foreach (['order', 'having', 'join', 'where'] as $group) {
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
        if (empty($cacheParentIds) || !$this->query instanceof CacheableBuilder) {
            return false;
        }

        $base = $this->query->toBase();
        $plan = $this->query->cachePlan($base, CachePlanContext::pivot(
            $this->selectedRelatedColumns(['*']) ?? [],
            $this->query->inferAggregateDependencies()
        ));

        if ($plan->mode !== CacheMode::Result) {
            return false;
        }

        // whereRaw with ? params can't be separated from FK bindings — same SQL, different values would collide.
        foreach ($base->wheres as $where) {
            if (($where['type'] ?? null) === 'raw' && str_contains(($where['sql'] ?? ''), '?')) {
                return false;
            }
        }

        return true;
    }

    private function pivotTableKey(): string
    {
        return NormCache::tableKey(
            $this->parent->getConnection()->getName(),
            $this->table
        );
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
