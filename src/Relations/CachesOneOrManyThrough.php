<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;
use NormCache\Values\CachePlanContext;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!$this->shouldUseCache()) {
            return parent::get($columns);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $builder = $this->prepareQueryBuilder($columns);

        $classification = ProjectionClassifier::classifyForRelation(
            $builder,
            (array) $columns,
            $this->related->getTable(),
            $this->related->getKeyName()
        );

        $shouldCacheModels = $classification['shouldCacheRelatedModels'];
        $selectedColumns = $classification['selectedRelatedColumns'];

        if (!$shouldCacheModels && !$classification['relatedKeyInProjection']) {
            return parent::get($columns);
        }

        $hash = QueryHasher::forRelationQuery($builder, $this->getQualifiedFirstKeyName());
        $relatedClass = $this->related::class;
        $throughClass = $this->throughParent::class;
        $depClasses = [$throughClass];

        return NormCache::rescue(
            fn() => NormCache::executor()->runResult(
                fetch: fn() => NormCache::getResultCache($relatedClass, $depClasses, $hash, null, [], CacheKeyBuilder::K_THROUGH),
                waitForBuild: fn() => NormCache::waitForBuild(
                    'result', $relatedClass, $hash, null, $depClasses, [], CacheKeyBuilder::K_THROUGH
                ),
                onMiss: function ($result) use ($relatedClass, $throughClass, $shouldCacheModels, $debugbarStart, $columns) {
                    CacheReporter::queryMiss($relatedClass, $result->key, $debugbarStart,
                        ['through' => $throughClass], 'through miss');
                    $models = parent::get($columns);
                    $modelAttrs = [];
                    if ($shouldCacheModels) {
                        foreach ($models as $model) {
                            $attrs = $model->getRawOriginal();
                            unset($attrs['laravel_through_key']);
                            $modelAttrs[$model->getKey()] = $attrs;
                        }
                    }

                    return [$models, ['cachePayload' => $this->cachePayloadFromResult($models), 'modelAttrs' => $modelAttrs]];
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
                onHit: function ($result) use ($relatedClass, $builder, $selectedColumns, $throughClass, $debugbarStart) {
                    CacheReporter::queryHit($relatedClass, $result->key ?? '', $debugbarStart,
                        ['through' => $throughClass], 'through hit');

                    return $this->hydrateFromIds(
                        $result->payload['ids'], $relatedClass, $builder, $selectedColumns, $result->payload['throughKeys'] ?? []
                    );
                },
                onBuild: fn() => parent::get($columns),
            ),
            fn() => parent::get($columns)
        );
    }

    private function shouldUseCache(): bool
    {
        if (!$this->query instanceof CacheableBuilder) {
            return false;
        }

        $base = $this->query->toBase();
        $projection = ProjectionClassifier::resolve($base, null);

        $plan = $this->query->cachePlan($base, CachePlanContext::through(
            $projection ?? [],
            $this->query->inferAggregateDependencies()
        ));

        return $plan->mode === CacheMode::Result;
    }

    private function cachePayloadFromResult(Collection $result): array
    {
        return [
            'ids' => $result->modelKeys(),
            'throughKeys' => $result->mapWithKeys(fn($model) => [
                $model->getKey() => $model->getAttribute('laravel_through_key'),
            ])->all(),
        ];
    }

    private function hydrateFromIds(array $ids, string $relatedClass, Builder $builder, ?array $selectedColumns, array $throughKeys = []): Collection
    {
        $models = NormCache::getModels($ids, $relatedClass, $selectedColumns, null, $builder, false, $this->related);

        if ($throughKeys !== []) {
            foreach ($models as $model) {
                if (array_key_exists($model->getKey(), $throughKeys)) {
                    $model->setAttribute('laravel_through_key', $throughKeys[$model->getKey()]);
                }
            }
        }

        if ($models && $builder->getEagerLoads()) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection($models)
        );
    }
}
