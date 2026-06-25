<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Collection;
use NormCache\Enums\CacheStatus;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ResultCacheResult;
use NormCache\Values\ThroughCacheResult;

final class ExecutionEngine
{
    /**
     * @param  callable(): ResultCacheResult  $fetch
     * @param  callable(): (ResultCacheResult|null)  $waitForBuild
     * @param  callable(ResultCacheResult): array{Collection, mixed}  $onMiss
     * @param  callable(mixed, ResultCacheResult): void  $onStore
     * @param  callable(ResultCacheResult): Collection  $onHit
     * @param  callable(): Collection  $onBuild
     */
    public function runResult(
        callable $fetch,
        callable $waitForBuild,
        callable $onMiss,
        callable $onStore,
        callable $onHit,
        callable $onBuild,
    ): Collection {
        $result = $fetch();

        if ($result->status === CacheStatus::Building) {
            $result = $waitForBuild();

            if ($result === null) {
                return $onBuild();
            }
        }

        if ($result->status === CacheStatus::Hit) {
            return $onHit($result);
        }

        [$models, $payload] = $onMiss($result);
        $onStore($payload, $result);

        return $models;
    }

    /**
     * @param  callable(): PivotCacheResult  $fetch
     * @param  callable(): (PivotCacheResult|null)  $waitForBuild
     * @param  callable(): Collection  $onBuild
     * @param  callable(): (Collection|array{Collection, Collection})  $onMiss
     * @param  callable(Collection, PivotCacheResult): void  $onStore
     * @param  callable(PivotCacheResult): Collection  $onHit
     */
    public function runPivot(
        callable $fetch,
        callable $waitForBuild,
        callable $onBuild,
        callable $onMiss,
        callable $onStore,
        callable $onHit,
    ): Collection {
        $result = $fetch();

        if ($result->status === CacheStatus::Building) {
            $result = $waitForBuild();

            if ($result === null) {
                return $onBuild();
            }
        }

        if (empty($result->missedIds())) {
            return $onHit($result);
        }

        $miss = $onMiss();
        [$models, $cacheModels] = is_array($miss) ? $miss : [$miss, $miss];
        $onStore($cacheModels, $result);

        return $models;
    }

    /**
     * @param  callable(): ResultCacheResult  $fetch
     * @param  callable(): (ResultCacheResult|null)  $waitForBuild
     * @param  callable(): mixed  $compute  used for both miss execution and budget-exhausted fallback
     * @param  callable(mixed, ResultCacheResult): void  $onStore
     * @param  callable(ResultCacheResult): mixed  $onHit
     */
    public function runScalar(
        callable $fetch,
        callable $waitForBuild,
        callable $compute,
        callable $onStore,
        callable $onHit,
    ): mixed {
        $result = $fetch();

        if ($result->status === CacheStatus::Building) {
            $result = $waitForBuild();

            if ($result === null) {
                return $compute();
            }
        }

        if ($result->status === CacheStatus::Miss) {
            $value = $compute();
            $onStore($value, $result);

            return $value;
        }

        return $onHit($result);
    }

    /**
     * @param  callable(): QueryCacheResult  $fetch
     * @param  callable(): (QueryCacheResult|null)  $waitForBuild
     * @param  callable(): Collection  $onBuild
     * @param  callable(QueryCacheResult): Collection  $onMiss
     * @param  callable(QueryCacheResult): Collection  $onHit
     */
    public function runNormalized(
        callable $fetch,
        callable $waitForBuild,
        callable $onBuild,
        callable $onMiss,
        callable $onHit,
    ): Collection {
        $result = $fetch();

        if ($result->status === CacheStatus::Building) {
            $result = $waitForBuild();

            if ($result === null) {
                return $onBuild();
            }
        }

        if ($result->status === CacheStatus::Miss) {
            return $onMiss($result);
        }

        return $onHit($result);
    }

    /**
     * Same control flow as {@see self::runNormalized()}, kept as a separate method so call
     * sites get ThroughCacheResult-typed callables instead of QueryCacheResult.
     *
     * @param  callable(): ThroughCacheResult  $fetch
     * @param  callable(): (ThroughCacheResult|null)  $waitForBuild
     * @param  callable(): Collection  $onBuild
     * @param  callable(ThroughCacheResult): Collection  $onMiss
     * @param  callable(ThroughCacheResult): Collection  $onHit
     */
    public function runThrough(
        callable $fetch,
        callable $waitForBuild,
        callable $onBuild,
        callable $onMiss,
        callable $onHit,
    ): Collection {
        $result = $fetch();

        if ($result->status === CacheStatus::Building) {
            $result = $waitForBuild();

            if ($result === null) {
                return $onBuild();
            }
        }

        if ($result->status === CacheStatus::Miss) {
            return $onMiss($result);
        }

        return $onHit($result);
    }
}
