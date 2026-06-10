<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;
use NormCache\Support\RelationCacheGuards;
use NormCache\Values\CachePlanContext;
use NormCache\Values\PreparedQuery;

trait CachesOneOrManyThrough
{
    use CollectsRelatedModels;

    public function get($columns = ['*']): Collection
    {
        if (!$this->query instanceof CacheableBuilder) {
            return parent::get($columns);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $prepared = $this->query->prepareScopedQuery();
        $builder = $prepared->builder;
        $base = $prepared->base;
        $builder->addSelect(
            $this->shouldSelect($base->columns ? [] : $columns)
        );
        $prepared->applyBeforeCallbacks();

        $classification = ProjectionClassifier::classifyForRelation(
            $base,
            (array) $columns,
            $this->related->getTable(),
            $this->related->getKeyName()
        );

        $shouldCacheModels = $classification['shouldCacheRelatedModels'];
        $selectedColumns = $classification['selectedRelatedColumns'];

        if (!$this->shouldUseCache($builder, $base)
            || (!$shouldCacheModels && !$classification['relatedKeyInProjection'])) {
            return $this->getFromPreparedBuilder($prepared);
        }

        $hash = QueryHasher::forRelationQuery($builder, $this->getQualifiedFirstKeyName(), $base);
        $relatedClass = $this->related::class;
        $throughClass = $this->throughParent::class;
        $depClasses = [$throughClass];

        return NormCache::rescue(
            fn() => NormCache::engine()->runResult(
                fetch: fn() => NormCache::getResultCache($relatedClass, $depClasses, $hash, null, [], CacheKeyBuilder::K_THROUGH),
                waitForBuild: fn() => NormCache::waitForResultBuild(
                    $relatedClass, $hash, null, $depClasses, [], CacheKeyBuilder::K_THROUGH
                ),
                onMiss: function ($result) use ($relatedClass, $throughClass, $shouldCacheModels, $debugbarStart, $prepared) {
                    CacheReporter::queryMiss($relatedClass, $result->key, $debugbarStart,
                        ['through' => $throughClass], 'through miss');
                    $rawModels = $this->getFromPreparedBuilder($prepared, false);
                    $modelAttrs = [];
                    if ($shouldCacheModels) {
                        foreach ($rawModels as $model) {
                            $attrs = $model->getRawOriginal();
                            unset($attrs['laravel_through_key']);
                            $modelAttrs[$model->getKey()] = $attrs;
                        }
                    }

                    $cachePayload = $this->cachePayloadFromResult($rawModels);

                    return [
                        $prepared->applyAfterCallbacks($rawModels),
                        [
                            'cachePayload' => $cachePayload,
                            'modelAttrs' => $modelAttrs,
                        ],
                    ];
                },
                onStore: function ($data, $result) use ($relatedClass) {
                    NormCache::attempt(function () use ($data, $result, $relatedClass) {
                        if (NormCache::storeResultCache(
                            $result->key, $data['cachePayload'], $result->buildingKey, null,
                            $result->wakeKey, $result->versionKeys, $result->expectedVersions, $result->buildingToken
                        )) {
                            NormCache::cacheModelAttrs($relatedClass, $data['modelAttrs']);
                        }
                    });
                },
                onHit: function ($result) use ($relatedClass, $prepared, $selectedColumns, $throughClass, $debugbarStart) {
                    CacheReporter::queryHit($relatedClass, $result->key ?? '', $debugbarStart,
                        ['through' => $throughClass], 'through hit');

                    return $this->hydrateFromIds(
                        $result->payload['ids'], $relatedClass, $prepared, $selectedColumns, $result->payload['throughKeys'] ?? []
                    );
                },
                onBuild: fn() => $this->getFromPreparedBuilder($prepared),
            ),
            fn() => $this->getFromPreparedBuilder($prepared)
        );
    }

    private function shouldUseCache(CacheableBuilder $builder, Builder $base): bool
    {
        if ($this->isSimpleThroughQuery($base, $builder)) {
            return true;
        }

        $projection = ProjectionClassifier::resolve($base, null);

        $plan = $builder->cachePlan($base, CachePlanContext::through(
            $projection ?? [],
            $builder->inferAggregateDependencies()
        ));

        return $plan->mode === CacheMode::Result;
    }

    private function isSimpleThroughQuery(Builder $base, CacheableBuilder $builder): bool
    {
        // Standard HasManyThrough/HasOneThrough always has exactly one join (the intermediate table).
        // The planner grants a bypass relaxation for this shape; validate it directly here.
        if (RelationCacheGuards::blocksBypass($builder, $base)
            || count($base->joins ?? []) !== 1
            || ProjectionClassifier::hasCalculatedColumns($base->columns)) {
            return false;
        }

        return true;
    }

    private function cachePayloadFromResult(Collection $result): array
    {
        $ids = [];
        $throughKeys = [];

        foreach ($result as $model) {
            $id = $model->getKey();
            $ids[] = $id;
            $throughKeys[$id] = $model->getAttribute('laravel_through_key');
        }

        return [
            'ids' => $ids,
            'throughKeys' => $throughKeys,
        ];
    }

    private function hydrateFromIds(
        array $ids,
        string $relatedClass,
        PreparedQuery $prepared,
        ?array $selectedColumns,
        array $throughKeys = [],
    ): Collection {
        $builder = $prepared->builder;
        $models = NormCache::getModels($ids, $relatedClass, $selectedColumns, null, $builder, false, $this->related);

        if ($throughKeys !== []) {
            foreach ($models as $model) {
                $id = $model->getKey();
                if (array_key_exists($id, $throughKeys)) {
                    $model->setAttribute('laravel_through_key', $throughKeys[$id]);
                }
            }
        }

        if ($models && $builder->getEagerLoads()) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $prepared->applyAfterCallbacks($this->related->newCollection($models));
    }

    private function getFromPreparedBuilder(PreparedQuery $prepared, bool $applyAfterCallbacks = true): Collection
    {
        return $this->collectFromPreparedBuilder($prepared, $applyAfterCallbacks);
    }
}
