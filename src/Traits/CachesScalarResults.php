<?php

namespace NormCache\Traits;

use NormCache\CacheableBuilder;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryInspector;

/**
 * @mixin CacheableBuilder
 */
trait CachesScalarResults
{
    public function count($columns = '*'): int
    {
        if ($columns !== '*' && !is_string($columns)) {
            return (int) parent::count($columns);
        }

        $kind = $columns === '*' ? 'count' : 'count:' . $columns;

        return (int) $this->cacheScalar($kind, fn() => parent::count($columns));
    }

    public function sum($column): mixed
    {
        return $this->cacheScalar('sum', fn() => parent::sum($column), $column);
    }

    public function avg($column): mixed
    {
        return $this->cacheScalar('avg', fn() => parent::avg($column), $column);
    }

    public function average($column): mixed
    {
        return $this->avg($column);
    }

    public function min($column): mixed
    {
        return $this->cacheScalar('min', fn() => parent::min($column), $column);
    }

    public function max($column): mixed
    {
        return $this->cacheScalar('max', fn() => parent::max($column), $column);
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
        if (!empty($this->pendingAggregates)) {
            $this->replayPendingAggregates();
        }

        return $this->cacheScalar('pluck', fn() => parent::pluck($column, $key), $column, $key);
    }

    public function value($column): mixed
    {
        if (!empty($this->pendingAggregates)) {
            $this->replayPendingAggregates();
        }

        return $this->cacheScalar('value', fn() => parent::value($column), $column);
    }

    private function cacheScalar(string $kindPrefix, \Closure $fallback, mixed ...$columns): mixed
    {
        foreach ($columns as $col) {
            if ($col !== null && !QueryInspector::isCacheableScalarColumn($col)) {
                return $fallback();
            }
        }

        $nonNull = array_filter($columns, fn($c) => $c !== null);
        $kind = $nonNull !== [] ? $kindPrefix . ':' . implode(':', $nonNull) : $kindPrefix;

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
            ['key' => $cacheKey, 'data' => $result] = NormCache::getNamespacedCache(
                'scalar',
                $this->model::class,
                $hash,
                $this->dependsOn ?? [],
                $this->cacheTag
            );
            $hit = $result !== null;
            $result = $hit ? $result[0] : null;

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
