<?php

namespace NormCache\Traits;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Str;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;
use NormCache\Values\CachePlanContext;

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

        return (int) $this->cacheScalar(
            $kind,
            fn() => parent::count($columns),
            (array) $columns,
            fn(QueryBuilder $base) => $base->count($columns)
        );
    }

    public function sum($column): mixed
    {
        return $this->cacheScalar(
            'sum',
            fn() => parent::sum($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->sum($column)
        );
    }

    public function avg($column): mixed
    {
        return $this->cacheScalar(
            'avg',
            fn() => parent::avg($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->avg($column)
        );
    }

    public function average($column): mixed
    {
        return $this->avg($column);
    }

    public function min($column): mixed
    {
        return $this->cacheScalar(
            'min',
            fn() => parent::min($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->min($column)
        );
    }

    public function max($column): mixed
    {
        return $this->cacheScalar(
            'max',
            fn() => parent::max($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->max($column)
        );
    }

    public function exists(): bool
    {
        return (bool) $this->cacheScalar(
            'exists',
            fn() => parent::exists() ? 1 : 0,
            compute: fn(QueryBuilder $base) => $base->exists() ? 1 : 0
        );
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

        return $this->cacheScalar(
            'pluck',
            fn() => parent::pluck($column, $key),
            $columns,
            fn(QueryBuilder $base) => $this->pluckFromPreparedBase($base, $column, $key)
        );
    }

    public function value($column): mixed
    {
        return $this->cacheScalar(
            'value',
            fn() => parent::value($column),
            (array) $column,
            fn(QueryBuilder $base) => $this->valueFromPreparedBase($base, $column)
        );
    }

    private function cacheScalar(
        string $kind,
        \Closure $fallback,
        array $columns = [],
        ?\Closure $compute = null,
    ): mixed {
        if (ProjectionClassifier::hasCalculatedColumns($columns)) {
            return $fallback();
        }

        if (in_array($kind, ['pluck', 'value'], true) && $this->hasAfterQueryCallbacks()) {
            return $fallback();
        }

        $prepared = $this->prepareCacheExecution();
        $executionBuilder = $prepared->builder;
        $base = $prepared->base;
        $computeValue = $compute === null
            ? $fallback
            : fn() => $compute($base);
        $plan = $executionBuilder->cachePlan($base, CachePlanContext::scalar(
            $kind,
            $columns,
            $executionBuilder->inferAggregateDependencies()
        ));

        if ($plan->mode === CacheMode::Bypass) {
            if (!$plan->hasBypassReason('opted_out')) {
                CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons);
            }

            return $computeValue();
        }

        $modelClass = $this->model::class;
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;
        $namespace = str_starts_with($kind, 'count') ? CacheKeyBuilder::K_COUNT : CacheKeyBuilder::K_SCALAR;
        $hash = QueryHasher::forScalarQuery($executionBuilder, $base, $kind, $plan->columns ?? []);
        $ttl = $this->getQueryTtl();
        $tag = $this->getCacheTag();
        $debugbarStart = CacheReporter::beginMeasure();

        return NormCache::executor()->runScalar(
            fetch: fn() => NormCache::getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace),
            waitForBuild: fn() => NormCache::waitForBuild('result', $modelClass, $hash, tag: $tag, depClasses: $depClasses, depTableKeys: $depTableKeys, namespace: $namespace),
            compute: $computeValue,
            onStore: function ($value, $result) use ($modelClass, $ttl, $debugbarStart, $kind) {
                CacheReporter::queryMiss($modelClass, $result->key, $debugbarStart, ['kind' => $kind]);

                NormCache::storeResultCache(
                    $result->key,
                    [$value],
                    $result->buildingKey,
                    $ttl,
                    $result->wakeKey,
                    $result->versionKeys,
                    $result->expectedVersions,
                    $result->buildingToken
                );
            },
            onHit: function ($result) use ($modelClass, $debugbarStart, $kind, $computeValue) {
                if (!is_array($result->payload) || !array_key_exists(0, $result->payload)) {
                    return $computeValue();
                }

                CacheReporter::queryHit($modelClass, $result->key, $debugbarStart, ['kind' => $kind]);

                return $result->payload[0];
            },
        );
    }

    private function pluckFromPreparedBase(QueryBuilder $base, mixed $column, mixed $key): mixed
    {
        $results = $base->pluck($column, $key);
        $column = $column instanceof Expression ? $column->getValue($this->getGrammar()) : $column;
        $column = Str::after((string) $column, $this->model->getTable() . '.');

        if (!$this->model->hasAnyGetMutator($column)
            && !$this->model->hasCast($column)
            && !in_array($column, $this->model->getDates())) {
            return $results;
        }

        return $results->map(
            fn($value) => $this->model->newFromBuilder([$column => $value])->{$column}
        );
    }

    private function valueFromPreparedBase(QueryBuilder $base, mixed $column): mixed
    {
        $result = $base->first([$column]);

        if ($result === null) {
            return null;
        }

        $column = $column instanceof Expression ? $column->getValue($this->getGrammar()) : $column;
        $column = Str::afterLast((string) $column, '.');
        $value = $result->{$column};

        if (!$this->model->hasAnyGetMutator($column)
            && !$this->model->hasCast($column)
            && !in_array($column, $this->model->getDates())) {
            return $value;
        }

        return $this->model->newFromBuilder([$column => $value])->{$column};
    }
}
