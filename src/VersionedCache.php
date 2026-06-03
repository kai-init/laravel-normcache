<?php

namespace NormCache;

use Closure;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlan;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;

final class VersionedCache
{
    public function rememberCollection(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlan $plan,
        mixed $columns,
        ?int $ttl,
        ?string $tag,
    ): Collection {
        $debugbarStart = CacheReporter::beginMeasure();
        $modelClass = $builder->getModel()::class;
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;
        $usesEloquentResult = $builder->hasAggregateColumns();

        if (!$usesEloquentResult) {
            $builder->prepareResultCacheQuery($base);
        }

        $hash = QueryHasher::forResultQuery($base);
        $result = NormCache::getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild('result', $modelClass, $hash, tag: $tag, depClasses: $depClasses, depTableKeys: $depTableKeys);

            if ($result === null) {
                CacheReporter::queryMiss($modelClass, 'building:budget-exhausted', $debugbarStart, ['kind' => 'result']);

                return $builder->getWithoutCache($columns);
            }
        }

        if ($result['status'] === 'miss') {
            CacheReporter::queryMiss($modelClass, $result['key'], $debugbarStart, ['kind' => 'result']);

            if ($usesEloquentResult) {
                $models = $builder->getWithoutCache($columns);
                $payload = $builder->resultPayloadFromEloquentModels($models);
            } else {
                $payload = $builder->buildResultPayloadFromQuery($base);
                $models = $builder->hydrateResultPayload($payload, $modelClass, false);
            }

            NormCache::storeResultCache(
                $result['key'],
                $payload,
                $result['buildingKey'],
                $ttl,
                $result['wakeKey'] ?? null,
                $result['versionKeys'],
                $result['expectedVersions'],
                $result['buildingToken'] ?? null
            );

            return $models;
        }

        CacheReporter::queryHit($modelClass, $result['key'], $debugbarStart, [
            'kind' => 'result',
            'contains' => class_basename($modelClass) . ' (' . count($result['payload']) . ' models)',
        ]);

        return $builder->finalizeResult(NormCache::hydrateResult($result['payload'], $modelClass));
    }

    public function rememberScalar(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlan $plan,
        Closure $fallback,
        string $kind,
        ?int $ttl,
        ?string $tag,
        string $namespace = CacheKeyBuilder::K_SCALAR,
    ): mixed {
        $debugbarStart = CacheReporter::beginMeasure();
        $modelClass = $builder->getModel()::class;
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;

        $hash = QueryHasher::forScalarQuery($base, $kind, $plan->columns ?? []);

        $result = NormCache::getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild('result', $modelClass, $hash, tag: $tag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: $namespace);

            if ($result === null) {
                return $fallback();
            }
        }

        if ($result['status'] === 'miss') {
            $value = $fallback();

            CacheReporter::queryMiss($modelClass, $result['key'], $debugbarStart, ['kind' => $kind]);

            NormCache::storeResultCache(
                $result['key'],
                [$value],
                $result['buildingKey'],
                $ttl,
                $result['wakeKey'] ?? null,
                $result['versionKeys'],
                $result['expectedVersions'],
                $result['buildingToken'] ?? null
            );

            return $value;
        }

        CacheReporter::queryHit($modelClass, $result['key'], $debugbarStart, ['kind' => $kind]);

        return $result['payload'][0] ?? null;
    }

    public function rememberPaginationCount(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlan $plan,
        ?int $ttl,
        ?string $tag,
    ): int {
        $debugbarStart = CacheReporter::beginMeasure();
        $modelClass = $builder->getModel()::class;
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;

        $hash = QueryHasher::forNormalizedQuery($base);

        $result = NormCache::getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys, CacheKeyBuilder::K_COUNT);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild('result', $modelClass, $hash, tag: $tag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: CacheKeyBuilder::K_COUNT);

            if ($result === null) {
                return $base->getCountForPagination();
            }
        }

        if ($result['status'] === 'miss') {
            CacheReporter::queryMiss($modelClass, $result['key'], $debugbarStart, ['kind' => 'pagination count']);

            $total = $base->getCountForPagination();
            NormCache::storeResultCache(
                $result['key'],
                [$total],
                $result['buildingKey'],
                $ttl,
                $result['wakeKey'],
                $result['versionKeys'],
                $result['expectedVersions'],
                $result['buildingToken'] ?? null
            );

            return $total;
        }

        CacheReporter::queryHit($modelClass, $result['key'], $debugbarStart, ['kind' => 'pagination count']);

        return (int) ($result['payload'][0] ?? 0);
    }
}
