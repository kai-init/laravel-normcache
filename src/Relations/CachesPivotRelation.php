<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Arr;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheKind;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;
use NormCache\Support\RawAttributes;
use NormCache\Values\BuildHandle;
use NormCache\Values\CachePlanContext;
use NormCache\Values\CacheSpace;
use NormCache\Values\PreparedQuery;

/** @mixin BelongsToMany */
trait CachesPivotRelation
{
    private array $eagerParentIds = [];

    private bool $inEagerLoad = false;

    private array $prebuiltDictionary = [];

    public function match(array $models, Collection $results, $relation)
    {
        if (!empty($this->prebuiltDictionary)) {
            $dictionary = $this->prebuiltDictionary;
            $this->prebuiltDictionary = [];

            foreach ($models as $model) {
                if (isset($dictionary[$key = $model->getAttribute($this->parentKey)])) {
                    $model->setRelation(
                        $relation, $this->related->newCollection($dictionary[$key])
                    );
                }
            }

            return $models;
        }

        return parent::match($models, $results, $relation);
    }

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

        if (!$this->query instanceof CacheableBuilder) {
            return parent::get($columns);
        }

        $prepared = $this->query->prepareScopedQuery();
        $builder = $prepared->builder;
        $base = $prepared->base;

        $classification = ProjectionClassifier::classifyForRelation(
            $base,
            $columns,
            $this->related->getTable(),
            $this->related->getKeyName()
        );

        $selectColumns = $base->columns ? [] : $columns;
        $builder->addSelect($this->shouldSelect($selectColumns));
        $prepared->applyBeforeCallbacks();
        $shouldCacheRelatedModels = $classification['shouldCacheRelatedModels'];
        $selectedRelatedColumns = $classification['selectedRelatedColumns'];

        $constraintHash = QueryHasher::forRelationQuery($this->getQualifiedForeignPivotKeyName(), $base);

        if (!$this->shouldUsePivotCache($cacheParentIds, $classification['resolvedColumns'], $builder, $base)
            || (!$shouldCacheRelatedModels && !$classification['relatedKeyInProjection'])) {
            return $this->getFromPreparedPivotBuilder($prepared);
        }

        $debugbarStart = CacheReporter::beginMeasure();
        $ttl = $builder->getQueryTtl();

        $parentClass = $this->parent::class;
        $relatedClass = $this->related::class;
        $parentClassKey = NormCache::keys()->classKey($parentClass);
        $relatedConnection = $this->related->getConnection()->getName()
            ?? $this->related->getConnectionName()
            ?? '';

        $runPivot = fn() => CacheFallback::rescue(
            NormCache::config(),
            fn() => NormCache::relationIndexes()->runPivot(
                parentClass: $parentClass,
                relatedClass: $relatedClass,
                relation: $this->relationName,
                parentIds: $cacheParentIds,
                constraintHash: $constraintHash,
                pivotTableKey: $this->pivotTableKey(),
                connection: $relatedConnection,
                onBuild: fn() => $this->getFromPreparedPivotBuilder($prepared),
                onMiss: function ($pivotResult) use ($parentClass, $parentClassKey, $relatedClass, $cacheParentIds, $debugbarStart, $prepared) {
                    CacheReporter::queryMiss($parentClass, "pivot:{$parentClassKey}:{$this->relationName}", $debugbarStart, [
                        ...CacheReporter::cacheMeta(CacheKind::RelationIndex, $pivotResult->status, ResultKind::Collection, NormCache::keys()->activeSpace()),
                        ...$pivotResult->meta,
                        'parents' => $cacheParentIds,
                        'related' => $relatedClass,
                    ], 'pivot miss');

                    $rawModels = $this->getFromPreparedPivotBuilder($prepared, false);

                    return [
                        $prepared->applyAfterCallbacks($rawModels),
                        $rawModels,
                    ];
                },
                onStore: function ($models, $pivotResult) use ($cacheParentIds, $parentClassKey, $relatedClass, $relatedConnection, $constraintHash, $shouldCacheRelatedModels, $ttl) {
                    $space = NormCache::keys()->activeSpace();

                    CacheFallback::attempt(
                        NormCache::config(),
                        function () use ($models, $cacheParentIds, $parentClassKey, $relatedClass, $relatedConnection, $constraintHash, $pivotResult, $shouldCacheRelatedModels, $ttl, $space) {
                            $relatedKey = NormCache::keys()->classKey($relatedClass, $relatedConnection);
                            $keyMap = [];
                            foreach ($cacheParentIds as $parentId) {
                                $keyMap[$parentId] = NormCache::keys()->pivotKey(
                                    $parentClassKey, $relatedKey, $this->relationName,
                                    $constraintHash, $pivotResult->seg, $parentId
                                );
                            }
                            $this->populatePivotCache(
                                $models,
                                $keyMap,
                                $relatedClass,
                                $shouldCacheRelatedModels,
                                $ttl,
                                $pivotResult->build,
                                $space,
                                $relatedConnection,
                            );
                        },
                    );
                },
                onHit: function ($pivotResult) use ($relatedClass, $relatedConnection, $selectedRelatedColumns, $parentClass, $parentClassKey, $cacheParentIds, $debugbarStart, $prepared) {
                    $matchStarted = CacheReporter::active() ? microtime(true) : null;
                    $resolvedVersion = isset($pivotResult->build->expectedVersions[0])
                        ? (int) $pivotResult->build->expectedVersions[0]
                        : null;
                    $models = $this->hydrateFromPivotCache(
                        $pivotResult->data,
                        $relatedClass,
                        $relatedConnection,
                        $selectedRelatedColumns,
                        $prepared,
                        $resolvedVersion,
                    );
                    CacheReporter::queryHit($parentClass, "pivot:{$parentClassKey}:{$this->relationName}", $debugbarStart, [
                        ...CacheReporter::cacheMeta(CacheKind::RelationIndex, $pivotResult->status, ResultKind::Collection, NormCache::keys()->activeSpace()),
                        ...$pivotResult->meta,
                        'parents' => $cacheParentIds,
                        'related' => $relatedClass,
                        'relation_match_time_ms' => $matchStarted === null ? null : (microtime(true) - $matchStarted) * 1000,
                    ], 'pivot hit');

                    return $models;
                },
            ),
            fn() => $this->getFromPreparedPivotBuilder($prepared)
        );

        return NormCache::withSpaceForModel($relatedClass, $builder->getSpace(), $runPivot);
    }

    private function shouldUsePivotCache(
        array $cacheParentIds,
        ?array $resolvedColumns,
        CacheableBuilder $builder,
        QueryBuilder $base,
    ): bool {
        if (empty($cacheParentIds)) {
            return false;
        }

        if ($builder->hasExplicitDependencies()) {
            return false;
        }

        if ($builder->getCacheTag() !== null) {
            return false;
        }

        $plan = $builder->cachePlan($base, CachePlanContext::pivot($resolvedColumns ?? []));

        if (!$plan->usesResultCache()) {
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
        return NormCache::keys()->tableKey(
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

    private function populatePivotCache(
        Collection $results,
        array $keyMap,
        string $relatedClass,
        bool $cacheRelatedModels,
        ?int $ttl,
        BuildHandle $build,
        ?CacheSpace $space = null,
        ?string $connection = null,
    ): void {
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
                $attrs = [];
                foreach ($model->getRawOriginal() as $key => $value) {
                    if (!str_starts_with($key, $pivotPrefix)) {
                        $attrs[$key] = $value;
                    }
                }

                $modelAttrs[$model->getKey()] = $attrs;
            }
        }

        $pivotEntriesByKey = [];
        foreach ($pivotMap as $parentId => $entries) {
            $pivotEntriesByKey[$keyMap[$parentId]] = $entries;
        }

        $stored = NormCache::relationIndexes()->storePivotEntries(
            $pivotEntriesByKey,
            $ttl,
            $build,
            $relatedClass,
        );

        if ($stored) {
            NormCache::modelCache()->storeForBuild(
                $relatedClass,
                $modelAttrs,
                $build,
                $space,
                $connection,
            );
        }
    }

    private function hydrateFromPivotCache(
        array $cachedByParentId,
        string $relatedClass,
        ?string $relatedConnection,
        ?array $selectedRelatedColumns,
        PreparedQuery $prepared,
        ?int $resolvedVersion,
    ): Collection {
        $uniqueRelatedIds = [];
        foreach ($cachedByParentId as $entries) {
            foreach ($entries as $entry) {
                $uniqueRelatedIds[$entry['id']] = true;
            }
        }

        $modelsById = [];
        $getAttribute = RawAttributes::getAttributeClosure();
        $keyName = $this->related->getKeyName();
        $ids = array_keys($uniqueRelatedIds);
        $raw = $resolvedVersion === null
            ? null
            : NormCache::modelCache()->rawForVersion($relatedClass, $ids, $resolvedVersion, $relatedConnection);
        foreach (NormCache::modelCache()->getModels(
            $ids,
            $relatedClass,
            $selectedRelatedColumns,
            $raw,
            resolvedVersion: $resolvedVersion,
            prototype: $this->related,
        ) as $model) {
            $modelsById[$getAttribute($model, $keyName)] = $model;
        }

        $result = [];
        $dictionary = [];
        $templatePivot = $this->newExistingPivot([]);

        foreach ($cachedByParentId as $parentId => $entries) {
            foreach ($entries as $entry) {
                if (!isset($modelsById[$entry['id']])) {
                    continue;
                }

                $model = clone $modelsById[$entry['id']];

                $pivot = clone $templatePivot;
                RawAttributes::hydrateClosure()($pivot, $entry['pivot'], false);

                $model->setRelation($this->accessor, $pivot);

                $result[] = $model;
                $dictionary[$parentId][] = $model;
            }
        }

        $this->prebuiltDictionary = $dictionary;

        if ($result && $prepared->builder->getEagerLoads()) {
            $result = $prepared->builder->eagerLoadRelations($result);
        }

        return $prepared->applyAfterCallbacks($this->related->newCollection($result));
    }

    private function getFromPreparedPivotBuilder(
        PreparedQuery $prepared,
        bool $applyAfterCallbacks = true,
    ): Collection {
        return $prepared->collect(
            applyAfterCallbacks: $applyAfterCallbacks,
            beforeEagerLoad: fn(array $models) => $this->hydratePivotRelation($models),
        );
    }

    protected function hydratePivotRelation(array $models)
    {
        $template = null;

        foreach ($models as $model) {
            $values = $this->migratePivotAttributes($model);

            if ($template === null) {
                $pivot = $template = $this->newExistingPivot($values);
            } else {
                $pivot = clone $template;
                RawAttributes::hydrateClosure()($pivot, $values, false);
            }

            $model->setRelation($this->accessor, $pivot);
        }
    }
}
