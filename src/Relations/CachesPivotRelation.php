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

    private ?array $preEagerWhereBindings = null;

    public function addEagerConstraints(array $models): void
    {
        $this->inEagerLoad = true;
        $this->eagerParentIds = $this->getKeys($models, $this->parentKey);
        $this->preEagerWhereBindings = $this->query->toBase()->getRawBindings()['where'];
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

        // Pivot cache payload stores related model IDs; bypass when the PK isn't in the projection.
        if (!$shouldCacheRelatedModels && !$this->relatedKeyInProjection($columns)) {
            return parent::get($columns);
        }

        $cache = NormCache::rescue(
            fn() => NormCache::getPivotCache(
                $parentClass,
                $relatedClass,
                $this->relationName,
                $cacheParentIds,
                $constraintHash,
                $this->pivotTableKey()
            ),
            fn() => parent::get($columns)
        );

        if ($cache instanceof Collection) {
            return $cache;
        }

        $seg = $cache['seg'];
        $cachedByParentId = $cache['data'];
        $missedIds = [];
        foreach ($cachedByParentId as $parentId => $payload) {
            if (!is_array($payload)) {
                $missedIds[] = $parentId;
            }
        }

        if (empty($missedIds)) {
            return NormCache::rescue(
                function () use ($cachedByParentId, $relatedClass, $selectedRelatedColumns, $parentClass, $parentClassKey, $cacheParentIds, $debugbarStart) {
                    CacheReporter::queryHit($parentClass, "pivot:{$parentClassKey}:{$this->relationName}", $debugbarStart, [
                        'parents' => $cacheParentIds,
                        'related' => $relatedClass,
                    ], 'pivot hit');

                    return $this->hydrateFromPivotCache($cachedByParentId, $relatedClass, $selectedRelatedColumns);
                },
                fn() => parent::get($columns)
            );
        }

        CacheReporter::queryMiss($parentClass, "pivot:{$parentClassKey}:{$this->relationName}", $debugbarStart, [
            'parents' => $cacheParentIds,
            'related' => $relatedClass,
        ], 'pivot miss');

        $results = parent::get($columns);

        NormCache::attempt(function () use ($results, $cacheParentIds, $parentClassKey, $relatedClass, $constraintHash, $seg, $shouldCacheRelatedModels, $cache) {
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
        });

        return $results;
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

        foreach (['orders', 'limit', 'offset', 'groups', 'havings', 'distinct', 'unionLimit', 'unionOffset', 'lock'] as $property) {
            if ($base->{$property} !== null && $base->{$property} !== [] && $base->{$property} !== false) {
                $shape[$property] = $base->{$property};
            }
        }

        // JoinClause and union QueryBuilder objects are not safely JSON-encodable; normalize to scalars.
        if (!empty($base->joins)) {
            $shape['joins'] = array_map(static fn ($join) => [
                'type'     => $join->type ?? null,
                'table'    => is_string($join->table) ? $join->table : (string) $join->table,
                'sql'      => $join->toSql(),
                'bindings' => $join->getBindings(),
            ], $base->joins);
        }

        if (!empty($base->unions)) {
            $shape['unions'] = array_map(static fn ($union) => [
                'all' => $union['all'] ?? false,
                'sql' => $union['query']->toSql(),
            ], $base->unions);
        }

        if (!empty($base->unionOrders)) {
            $shape['unionOrders'] = $base->unionOrders;
        }

        $rawBindings = $base->getRawBindings();

        // For where bindings in eager-load mode, use the snapshot taken before addEagerConstraints().
        $whereBindings = $this->inEagerLoad ? ($this->preEagerWhereBindings ?? []) : ($rawBindings['where'] ?? []);
        if (!empty($whereBindings)) {
            $shape['bindings_where'] = $whereBindings;
        }
        foreach (['order', 'having', 'join'] as $group) {
            if (!empty($rawBindings[$group])) {
                $shape['bindings_' . $group] = $rawBindings[$group];
            }
        }

        if (empty($shape)) {
            return 'nc';
        }

        return QueryHasher::hash(json_encode($shape, JSON_THROW_ON_ERROR));
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

    private function relatedKeyInProjection(array $columns): bool
    {
        $cols = $this->query->toBase()->columns ?? $columns;
        if ($cols === ['*']) {
            return true;
        }
        $key = $this->related->getKeyName();
        $qualified = $this->related->getTable() . '.' . $key;

        return in_array($key, $cols, true) || in_array($qualified, $cols, true);
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

            if (isset($pivotMap[$parentId])) {
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
