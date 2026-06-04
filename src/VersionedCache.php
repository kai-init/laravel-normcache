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

        if (!$usesEloquentResult && !empty($base->joins) && empty($base->columns)) {
            if ($columns === ['*']) {
                CacheReporter::queryBypassed($modelClass, ['normalization' => ['join_result_requires_explicit_select']], $debugbarStart);

                return $builder->getWithoutCache($columns);
            }

            $base->columns = (array) $columns;
        }

        // Apply non-star columns so hash and payload reflect the actual projection.
        if ($base->columns === null && $columns !== ['*']) {
            $base->columns = (array) $columns;
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
        $modelClass = $builder->getModel()::class;

        return $this->rememberSingleValue(
            $modelClass,
            $plan->dependencies->depClassesFor($modelClass),
            $plan->dependencies->tables,
            QueryHasher::forScalarQuery($base, $kind, $plan->columns ?? []),
            $kind,
            $fallback,
            $ttl,
            $tag,
            $namespace,
        );
    }

    public function rememberPaginationCount(
        CacheableBuilder $builder,
        QueryBuilder $base,
        CachePlan $plan,
        ?int $ttl,
        ?string $tag,
    ): int {
        $modelClass = $builder->getModel()::class;

        return (int) $this->rememberSingleValue(
            $modelClass,
            $plan->dependencies->depClassesFor($modelClass),
            $plan->dependencies->tables,
            QueryHasher::forPaginationCountQuery($base),
            'pagination count',
            fn() => $base->getCountForPagination(),
            $ttl,
            $tag,
            CacheKeyBuilder::K_COUNT,
        );
    }

    private function rememberSingleValue(
        string $modelClass,
        array $depClasses,
        array $depTableKeys,
        string $hash,
        string $kind,
        callable $compute,
        ?int $ttl,
        ?string $tag,
        string $namespace,
    ): mixed {
        $debugbarStart = CacheReporter::beginMeasure();

        $result = NormCache::getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild('result', $modelClass, $hash, tag: $tag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: $namespace);

            if ($result === null) {
                return $compute();
            }
        }

        if ($result['status'] === 'miss') {
            $value = $compute();

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
}
