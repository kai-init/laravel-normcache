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
use NormCache\Support\QueryHasher;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!$this->shouldUseCache()) {
            return parent::get($columns);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $shouldCacheModels = $columns === ['*'] && $this->query->toBase()->columns === null;
        $builder = $this->prepareQueryBuilder($columns);
        $hash = QueryHasher::forResultQuery($builder->toBase());
        $relatedClass = $this->related::class;
        $throughClass = $this->throughParent::class;

        try {
            $result = NormCache::getResultCache($relatedClass, [$throughClass], $hash, null, [], CacheKeyBuilder::K_THROUGH);

            if ($result['status'] === 'building') {
                $result = NormCache::waitForBuild('result', $relatedClass, $hash, null, [$throughClass], [], CacheKeyBuilder::K_THROUGH);

                if ($result === null) {
                    return parent::get($columns);
                }
            }

            $key = $result['key'];

            if ($result['status'] === 'hit') {
                CacheReporter::queryHit($relatedClass, $key, $debugbarStart, [
                    'through' => $throughClass,
                ], 'through hit');

                return $this->hydrateFromIds(
                    $result['payload']['ids'],
                    $relatedClass,
                    $builder,
                    $this->projectionColumns($shouldCacheModels),
                    $result['payload']['throughKeys']
                );
            }

            CacheReporter::queryMiss($relatedClass, $key, $debugbarStart, [
                'through' => $throughClass,
            ], 'through miss');

            $models = parent::get($columns);
            $payload = $this->cachePayloadFromResult($models);

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

            return $models;
        } catch (\Throwable $e) {
            NormCache::fallback($e);

            return parent::get($columns);
        }
    }

    private function shouldUseCache(): bool
    {
        if (!$this->query instanceof CacheableBuilder) {
            return false;
        }

        $base = $this->query->toBase();
        $plan = $this->query->cachePlan($base, CachePlanContext::through(
            $this->projectionColumns(true) ?? [],
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

    private function projectionColumns(bool $shouldCacheModels): ?array
    {
        if ($shouldCacheModels) {
            return null;
        }

        $cols = $this->query->toBase()->columns;

        if ($cols === null || $cols === ['*']) {
            return null;
        }

        $hasWildcard = (bool) array_filter($cols, fn($c) => str_ends_with((string) $c, '*'));

        return $hasWildcard ? null : $cols;
    }

    private function hydrateFromIds(array $ids, string $relatedClass, Builder $builder, ?array $selectedColumns, array $throughKeys = []): Collection
    {
        $models = NormCache::getModels($ids, $relatedClass, $selectedColumns, null, $builder, false);

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
