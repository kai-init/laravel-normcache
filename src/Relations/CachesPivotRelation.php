<?php

namespace NormCache\Relations;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Query\Builder as QueryBuilder;
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
                $wheres[] = $this->normalizeWhereForHash($where);
            }
        }
        if ($wheres !== []) {
            $shape['wheres'] = $wheres;
        }

        foreach (['orders', 'limit', 'offset', 'groups', 'havings', 'distinct', 'unionLimit', 'unionOffset', 'unionOrders', 'lock'] as $property) {
            if ($base->{$property} !== null && $base->{$property} !== [] && $base->{$property} !== false) {
                $shape[$property] = $this->normalizeValueForHash($base->{$property});
            }
        }

        if (!empty($base->joins)) {
            $shape['joins'] = array_map(fn($join) => [
                'type' => $join->type ?? null,
                'table' => is_string($join->table) ? $join->table : (string) $join->table,
                'sql' => $join->toSql(),
                'bindings' => $this->normalizeValueForHash($join->getBindings()),
            ], $base->joins);
        }

        if (!empty($base->unions)) {
            $shape['unions'] = array_map(fn($union) => [
                'all' => $union['all'] ?? false,
                'sql' => $union['query']->toSql(),
                'bindings' => $this->normalizeValueForHash($union['query']->getBindings()),
            ], $base->unions);
        }

        $rawBindings = $base->getRawBindings();
        $whereBindings = $this->inEagerLoad ? ($this->preEagerWhereBindings ?? []) : $rawBindings['where'];
        if (!empty($whereBindings)) {
            $shape['bindings_where'] = $this->normalizeValueForHash($whereBindings);
        }
        foreach (['order', 'having', 'join'] as $group) {
            if (!empty($rawBindings[$group])) {
                $shape['bindings_' . $group] = $this->normalizeValueForHash($rawBindings[$group]);
            }
        }

        if (empty($shape)) {
            return 'nc';
        }

        return QueryHasher::hash(json_encode($shape, JSON_THROW_ON_ERROR));
    }

    private function normalizeValueForHash(mixed $value): mixed
    {
        if ($value instanceof QueryBuilder) {
            return [
                'sql' => $value->toSql(),
                'bindings' => $value->getBindings(),
            ];
        }

        if ($value instanceof Expression) {
            return [
                'expression' => $value->getValue($this->query->toBase()->getGrammar()),
            ];
        }

        if ($value instanceof \BackedEnum) {
            return $value->value;
        }

        if ($value instanceof \Stringable) {
            return (string) $value;
        }

        if (is_array($value)) {
            $normalized = [];
            foreach ($value as $key => $item) {
                $normalized[$key] = $this->normalizeValueForHash($item);
            }

            return $normalized;
        }

        if (is_object($value)) {
            return [
                'class' => $value::class,
                'value' => method_exists($value, '__toString') ? (string) $value : null,
            ];
        }

        return $value;
    }

    private function normalizeWhereForHash(array $where): array
    {
        $type = $where['type'] ?? null;

        if ($type === 'Nested' && isset($where['query'])) {
            return [
                'type' => 'Nested',
                'boolean' => $where['boolean'] ?? 'and',
                'wheres' => array_map(
                    fn($nested) => $this->normalizeWhereForHash($nested),
                    $where['query']->wheres ?? []
                ),
            ];
        }

        if (in_array($type, ['Exists', 'NotExists', 'Sub'], true) && isset($where['query'])) {
            return [
                'type' => $type,
                'boolean' => $where['boolean'] ?? 'and',
                'sql' => $where['query']->toSql(),
            ];
        }

        if ($type === 'Raw') {
            return [
                'type' => 'Raw',
                'sql' => $where['sql'] ?? null,
                'boolean' => $where['boolean'] ?? 'and',
            ];
        }

        return [
            'type' => $type,
            'column' => is_string($where['column'] ?? null) ? $where['column'] : null,
            'operator' => $where['operator'] ?? null,
            'boolean' => $where['boolean'] ?? 'and',
        ];
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

        if (array_filter($cols, static fn($c) => is_string($c) && str_ends_with($c, '*'))) {
            return true;
        }

        $key = $this->related->getKeyName();
        $qualified = $this->related->getTable() . '.' . $key;

        return in_array($key, $cols, true) || in_array($qualified, $cols, true);
    }

    private function shouldCacheRelatedModels(array $columns): bool
    {
        $queryColumns = $this->query->toBase()->columns;

        if ($queryColumns !== null) {
            return (bool) array_filter($queryColumns, static fn($c) => is_string($c) && str_ends_with($c, '*'));
        }

        return $columns === ['*'];
    }

    private function selectedRelatedColumns(array $columns): ?array
    {
        $queryColumns = $this->query->toBase()->columns;

        if ($queryColumns !== null) {
            $hasWildcard = (bool) array_filter($queryColumns, static fn($c) => is_string($c) && str_ends_with($c, '*'));

            return $hasWildcard ? null : $queryColumns;
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
