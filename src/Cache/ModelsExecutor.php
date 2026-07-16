<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheManager;
use NormCache\Enums\CacheStatus;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Values\BuildHandle;
use NormCache\Values\CachePlan;
use NormCache\Values\PreparedQuery;
use NormCache\Values\QueryCacheResult;

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

        return $prepared->finalizeModels(NormCache::hydrator()->getModels(
            $primaryKeys,
            $model,
            $selectedCols,
            null,
            $executionBuilder,
            false,
            $prototype
        ));
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
        $connection = $prototype->getConnectionName();
        /** @var CacheManager $manager */
        $manager = NormCache::getFacadeRoot();
        $queries = $manager->queries();
        $hydrator = $manager->hydrator();
        $result = $queries->fetch($model, $hash, $cacheTag, $depClasses, $depTableKeys, $connection);

        if ($result->status === CacheStatus::Hit) {
            return $this->resolveCachedQueryHit(
                $prepared,
                $hydrator,
                $result,
                $model,
                $selectedCols,
                $debugbarStart,
                $prototype,
                $executionBuilder,
            );
        }

        return $manager->engine()->runNormalized(
            fetch: static fn() => $result,
            waitForBuild: fn() => $queries->waitForBuild($model, $hash, $cacheTag, $depClasses, $depTableKeys, $connection),
            onBuild: function () use ($prepared, $executionBuilder, $base, $model, $selectedCols, $debugbarStart, $prototype, $hydrator) {
                CacheReporter::queryMiss($model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $prepared->finalizeModels(
                    $hydrator->getModels($this->buildIds($base, $prototype), $model, $selectedCols, null, $executionBuilder, true, $prototype)
                );
            },
            onMiss: function ($result) use ($prepared, $executionBuilder, $base, $model, $selectedCols, $debugbarStart, $queryTtl, $prototype, $queries, $hydrator) {
                CacheReporter::queryMiss($model, $result->key, $debugbarStart, ['kind' => 'ids']);

                $ids = $this->resolveIds(
                    $result->key,
                    $base,
                    $queryTtl,
                    $prototype,
                    $result->build,
                    $queries,
                );

                return $prepared->finalizeModels(
                    $hydrator->getModels($ids, $model, $selectedCols, null, $executionBuilder, true, $prototype)
                );
            },
            onHit: fn($result) => $this->resolveCachedQueryHit(
                $prepared,
                $hydrator,
                $result,
                $model,
                $selectedCols,
                $debugbarStart,
                $prototype,
                $executionBuilder,
            ),
        );
    }

    private function resolveCachedQueryHit(
        PreparedQuery $prepared,
        ModelHydrator $hydrator,
        QueryCacheResult $result,
        string $model,
        ?array $selectedCols,
        mixed $debugbarStart,
        Model $prototype,
        EloquentBuilder $executionBuilder,
    ): Collection {
        CacheReporter::queryHit($model, $result->key, $debugbarStart, [
            'kind' => 'ids + models',
            'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result->ids) . ' ids)',
            'contains_model' => $result->ids,
        ]);

        return $prepared->finalizeModels(
            $hydrator->getModels($result->ids, $model, $selectedCols, $result->models, $executionBuilder, true, $prototype)
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
        BuildHandle $build,
        NormalizedCacheRepository $queries,
    ): array {
        $ids = $this->buildIds($base, $prototype);
        $queries->store($key, $ids, $queryTtl, $build);

        return $ids;
    }
}
