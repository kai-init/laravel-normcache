<?php

namespace NormCache\Traits;

use NormCache\CacheableBuilder;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;

/**
 * @mixin CacheableBuilder
 */
trait CachesScalarResults
{
    public function count($columns = '*'): int
    {
        if ($columns !== '*') {
            return parent::count($columns);
        }

        if ($this->skipCache || !NormCache::isEnabled()) {
            return parent::count($columns);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $base = $this->toBase();
        $bypassReasons = $this->computeBypassReasons($base);

        if (!empty($bypassReasons)) {
            if (NormCache::isEventsEnabled()) {
                event(new QueryBypassed($this->model::class, $bypassReasons));
            }
            NormCacheCollector::recordBypass($this->model::class, $bypassReasons, $debugbarStart);

            return parent::count($columns);
        }

        $hash = $this->queryCacheKey($base);

        try {
            $cacheKey = NormCache::getNamespacedCache('count', $this->model::class, $hash)['key'];
            $cachedCount = NormCache::getQueryAggregate($cacheKey);
            $hit = $cachedCount !== null;

            if (!$hit) {
                $cachedCount = parent::count($columns);
                NormCache::storeQueryAggregate($cacheKey, $cachedCount, $this->queryTtl);
            }

            if (NormCache::isEventsEnabled()) {
                event($hit ? new QueryCacheHit($this->model::class, $cacheKey) : new QueryCacheMiss($this->model::class, $cacheKey));
            }

            NormCacheCollector::recordQuery($hit ? 'query hit' : 'query miss', $this->model::class, $cacheKey, $debugbarStart, ['kind' => 'count']);

            return (int) $cachedCount;
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return parent::count($columns);
        }
    }

    public function sum($column): mixed
    {
        return $this->cacheScalar("sum:{$column}", fn() => parent::sum($column));
    }

    public function avg($column): mixed
    {
        return $this->cacheScalar("avg:{$column}", fn() => parent::avg($column));
    }

    public function average($column): mixed
    {
        return $this->avg($column);
    }

    public function min($column): mixed
    {
        return $this->cacheScalar("min:{$column}", fn() => parent::min($column));
    }

    public function max($column): mixed
    {
        return $this->cacheScalar("max:{$column}", fn() => parent::max($column));
    }

    public function exists(): bool
    {
        return (bool) $this->cacheScalar('exists', fn() => parent::exists() ? 1 : 0);
    }

    public function doesntExist(): bool
    {
        return !$this->exists();
    }

    public function pluck($column, $key = null)
    {
        $kind = 'pluck:' . $column . ($key !== null ? ':' . $key : '');

        return $this->cacheScalar($kind, fn() => parent::pluck($column, $key));
    }

    public function value($column): mixed
    {
        return $this->cacheScalar('value:' . $column, fn() => parent::value($column));
    }

    private function cacheScalar(string $kind, \Closure $fallback): mixed
    {
        if ($this->skipCache || !NormCache::isEnabled()) {
            return $fallback();
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $base = $this->toBase();
        $bypassReasons = $this->computeBypassReasons($base);

        if (!empty($bypassReasons)) {
            if (NormCache::isEventsEnabled()) {
                event(new QueryBypassed($this->model::class, $bypassReasons));
            }

            NormCacheCollector::recordBypass($this->model::class, $bypassReasons, $debugbarStart);

            return $fallback();
        }

        $hash = $this->queryCacheKey($base) . ":{$kind}";

        try {
            $cacheKey = NormCache::getNamespacedCache('scalar', $this->model::class, $hash)['key'];
            $result = NormCache::getQueryAggregate($cacheKey);
            $hit = $result !== null;

            if (!$hit) {
                $result = $fallback();
                NormCache::storeQueryAggregate($cacheKey, $result, $this->queryTtl);
            }

            if (NormCache::isEventsEnabled()) {
                event($hit
                    ? new QueryCacheHit($this->model::class, $cacheKey)
                    : new QueryCacheMiss($this->model::class, $cacheKey)
                );
            }

            NormCacheCollector::recordQuery(
                $hit ? 'query hit' : 'query miss',
                $this->model::class,
                $cacheKey,
                $debugbarStart,
                ['kind' => $kind]
            );

            return $result;
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return $fallback();
        }
    }
}
