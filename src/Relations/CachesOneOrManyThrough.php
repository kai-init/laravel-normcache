<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlanContext;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;

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

        $result = NormCache::rescue(
            fn() => NormCache::getResultCache($relatedClass, [$throughClass], $hash, null, [], CacheKeyBuilder::K_THROUGH),
            fn() => parent::get($columns)
        );

        if ($result instanceof Collection) {
            return $result;
        }

        if ($result['status'] === 'building') {
            $result = NormCache::rescue(
                fn() => NormCache::waitForBuild('result', $relatedClass, $hash, null, [$throughClass], [], CacheKeyBuilder::K_THROUGH),
                fn() => parent::get($columns)
            );

            if ($result instanceof Collection) {
                return $result;
            }

            if ($result === null) {
                return parent::get($columns);
            }
        }

        $key = $result['key'];

        if ($result['status'] === 'hit') {
            return NormCache::rescue(
                function () use ($result, $relatedClass, $builder, $selectedColumns, $throughClass, $key, $debugbarStart) {
                    CacheReporter::queryHit($relatedClass, $key, $debugbarStart, [
                        'through' => $throughClass,
                    ], 'through hit');

                    return $this->hydrateFromIds(
                        $result['payload']['ids'],
                        $relatedClass,
                        $builder,
                        $selectedColumns,
                        $result['payload']['throughKeys']
                    );
                },
                fn() => parent::get($columns)
            );
        }

        CacheReporter::queryMiss($relatedClass, $key, $debugbarStart, [
            'through' => $throughClass,
        ], 'through miss');

        $models = parent::get($columns);
        $payload = $this->cachePayloadFromResult($models);

        NormCache::attempt(function () use ($models, $shouldCacheModels, $relatedClass, $key, $payload, $result) {
            $modelAttrs = [];
            if ($shouldCacheModels) {
                foreach ($models as $model) {
                    $attrs = $model->getRawOriginal();
                    unset($attrs['laravel_through_key']);
                    $modelAttrs[$model->getKey()] = $attrs;
                }
            }

            if (NormCache::storeResultCache($key, $payload, $result['buildingKey'], null, $result['wakeKey'], $result['versionKeys'], $result['expectedVersions'], $result['buildingToken'] ?? null)) {
                NormCache::cacheModelAttrs($relatedClass, $modelAttrs);
            }
        });

        return $models;
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
