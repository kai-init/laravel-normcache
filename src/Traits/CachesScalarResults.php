<?php

namespace NormCache\Traits;

use NormCache\Enums\CacheMode;
use NormCache\Planning\CachePlanContext;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;

/**
 * @mixin CacheableBuilder
 */
trait CachesScalarResults
{
    public function count($columns = '*'): int
    {
        if ($columns !== '*' && !is_string($columns)) {
            return parent::count($columns);
        }

        $kind = $columns === '*' ? CacheKeyBuilder::K_COUNT : CacheKeyBuilder::K_COUNT . ':' . $columns;

        return (int) $this->cacheScalar($kind, fn() => parent::count($columns), (array) $columns);
    }

    public function sum($column): mixed
    {
        return $this->cacheScalar('sum', fn() => parent::sum($column), (array) $column);
    }

    public function avg($column): mixed
    {
        return $this->cacheScalar('avg', fn() => parent::avg($column), (array) $column);
    }

    public function average($column): mixed
    {
        return $this->avg($column);
    }

    public function min($column): mixed
    {
        return $this->cacheScalar('min', fn() => parent::min($column), (array) $column);
    }

    public function max($column): mixed
    {
        return $this->cacheScalar('max', fn() => parent::max($column), (array) $column);
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
        $columns = [$column];
        if ($key !== null) {
            $columns[] = $key;
        }

        return $this->cacheScalar('pluck', fn() => parent::pluck($column, $key), $columns);
    }

    public function value($column): mixed
    {
        return $this->cacheScalar('value', fn() => parent::value($column), (array) $column);
    }

    private function cacheScalar(string $kind, \Closure $fallback, array $columns = []): mixed
    {
        if (QueryAnalyzer::hasCalculatedColumns($columns)) {
            return $fallback();
        }

        $base = $this->toBase();
        $plan = $this->cachePlan($base, CachePlanContext::scalar($kind, $columns, $this->inferAggregateDependencies()));

        if ($plan->mode === CacheMode::Bypass) {
            if (!$plan->hasBypassReason('opted_out')) {
                CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons);
            }

            return $fallback();
        }

        return $this->versionedCache()->rememberScalar(
            $this,
            $base,
            $plan,
            $fallback,
            $kind,
            $this->getQueryTtl(),
            $this->getCacheTag(),
            str_starts_with($kind, 'count') ? CacheKeyBuilder::K_COUNT : CacheKeyBuilder::K_SCALAR
        );
    }
}
