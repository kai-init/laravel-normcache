<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Values\CachePlan;
use NormCache\Values\PreparedQuery;

final class ModelsExecutor
{
    public function runDirect(
        PreparedQuery $prepared,
        array $primaryKeys,
        string $model,
        ?array $selectedCols,
        Model $prototype,
    ): Collection {
        $executionBuilder = $prepared->builder;

        return $executionBuilder->finalizeResult(NormCache::getModels(
            $primaryKeys,
            $model,
            $selectedCols,
            null,
            $executionBuilder,
            false,
            $prototype
        ), $prepared);
    }

    public function runNormalized(
        PreparedQuery $prepared,
        CachePlan $plan,
        string $model,
        ?array $selectedCols,
        ?string $cacheTag,
        ?int $queryTtl,
        mixed $debugbarStart,
        Model $prototype,
    ): Collection {
        $executionBuilder = $prepared->builder;
        $base = $prepared->base;
        $hash = QueryHasher::forNormalizedQuery($executionBuilder, $base);
        $depClasses = $plan->dependencies->depClassesFor($model);
        $depTableKeys = $plan->dependencies->tables;

        return NormCache::engine()->runNormalized(
            fetch: fn() => NormCache::getModelsFromQuery($model, $hash, $cacheTag, $depClasses, $depTableKeys),
            waitForBuild: fn() => NormCache::waitForQueryBuild($model, $hash, $cacheTag, $depClasses, $depTableKeys),
            onBuild: function () use ($prepared, $executionBuilder, $base, $model, $selectedCols, $debugbarStart, $prototype) {
                CacheReporter::queryMiss($model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $executionBuilder->finalizeResult(
                    NormCache::getModels($this->buildIds($base, $prototype), $model, $selectedCols, null, $executionBuilder, true, $prototype),
                    $prepared
                );
            },
            onMiss: function ($result) use ($prepared, $executionBuilder, $base, $model, $selectedCols, $debugbarStart, $queryTtl, $prototype) {
                CacheReporter::queryMiss($model, $result->key, $debugbarStart, ['kind' => 'ids']);

                $ids = $this->resolveIds(
                    $result->key, $base, $queryTtl, $prototype,
                    $result->buildingKey, $result->versionKeys, $result->expectedVersions, $result->buildingToken, $result->wakeKey
                );

                return $executionBuilder->finalizeResult(
                    NormCache::getModels($ids, $model, $selectedCols, null, $executionBuilder, true, $prototype),
                    $prepared
                );
            },
            onHit: function ($result) use ($prepared, $executionBuilder, $model, $selectedCols, $debugbarStart, $prototype) {
                CacheReporter::queryHit($model, $result->key, $debugbarStart, [
                    'kind' => 'ids + models',
                    'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result->ids) . ' ids)',
                    'contains_model' => $result->ids,
                ]);

                return $executionBuilder->finalizeResult(
                    NormCache::getModels($result->ids, $model, $selectedCols, $result->models, $executionBuilder, true, $prototype),
                    $prepared
                );
            },
        );
    }

    private function buildIds(QueryBuilder $base, Model $prototype): array
    {
        return $base
            ->cloneWithout(['columns'])
            ->cloneWithoutBindings(['select'])
            ->select($prototype->getKeyName())
            ->pluck($prototype->getKeyName())
            ->all();
    }

    private function resolveIds(
        string $key,
        QueryBuilder $base,
        ?int $queryTtl,
        Model $prototype,
        ?string $buildingKey = null,
        array $versionKeys = [],
        array $expectedVersions = [],
        ?string $buildingToken = null,
        ?string $wakeKey = null,
    ): array {
        $ids = $this->buildIds($base, $prototype);
        NormCache::storeQueryIds($key, $ids, $queryTtl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken, $wakeKey);

        return $ids;
    }
}
