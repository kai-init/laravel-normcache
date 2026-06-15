<?php

namespace NormCache\Cache;

use Closure;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Values\CachePlan;
use NormCache\Values\PreparedQuery;

final class ResultExecutor
{
    public function execute(
        PreparedQuery $prepared,
        CachePlan $plan,
        ResultKind $kind,
        array $columns,
        Closure $compute,
    ): array {
        $builder = $prepared->builder;
        $modelClass = $builder->getModel()::class;
        $tag = $builder->getCacheTag();
        $ttl = $builder->getQueryTtl();
        $debugbarStart = CacheReporter::beginMeasure();

        $namespace = $this->resolveNamespace($kind);
        $hash = $this->resolveHash($prepared, $kind, $columns);
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;

        $cached = false;

        $value = NormCache::rescue(
            fn() => NormCache::engine()->runScalar(
                fetch: fn() => NormCache::getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace),
                waitForBuild: fn() => NormCache::waitForResultBuild($modelClass, $hash, tag: $tag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: $namespace),
                compute: $compute,
                onStore: function ($value, $result) use ($modelClass, $ttl, $debugbarStart, $kind) {
                    CacheReporter::queryMiss($modelClass, $result->key, $debugbarStart, ['kind' => $kind->value]);

                    NormCache::storeResultCache(
                        $result->key,
                        is_array($value) ? $value : [$value],
                        $result->buildingKey,
                        $ttl,
                        $result->wakeKey,
                        $result->versionKeys,
                        $result->expectedVersions,
                        $result->buildingToken
                    );
                },
                onHit: function ($result) use ($modelClass, $debugbarStart, $kind, $compute, &$cached) {
                    if (!is_array($result->payload) || ($kind !== ResultKind::Collection && !array_key_exists(0, $result->payload))) {
                        return $compute();
                    }

                    $cached = true;
                    CacheReporter::queryHit($modelClass, $result->key, $debugbarStart, ['kind' => $kind->value]);

                    return $kind === ResultKind::Collection
                        ? $result->payload
                        : $result->payload[0];
                },
            ),
            $compute
        );

        return [$value, $cached];
    }

    private function resolveNamespace(ResultKind $kind): string
    {
        return match ($kind) {
            ResultKind::Count, ResultKind::PaginationCount => CacheKeyBuilder::K_COUNT,
            ResultKind::Collection => CacheKeyBuilder::K_RESULT,
            default => CacheKeyBuilder::K_SCALAR,
        };
    }

    private function resolveHash(PreparedQuery $prepared, ResultKind $kind, array $columns): string
    {
        $query = $prepared->base;

        if ($kind === ResultKind::Collection) {
            if (empty($query->columns) && $columns !== ['*']) {
                $query = $query->cloneWithout([]);
                $query->columns = $columns;
            }

            return QueryHasher::forResultQuery($prepared->builder, $query);
        }

        return match ($kind) {
            ResultKind::PaginationCount => QueryHasher::forPaginationCountQuery($prepared->builder, $query),
            default => QueryHasher::forScalarQuery($prepared->builder, $query, $kind->value, $columns),
        };
    }
}
