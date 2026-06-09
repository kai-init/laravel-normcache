<?php

namespace NormCache\Traits;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Str;
use NormCache\Cache\ModelHydrator;
use NormCache\Enums\CacheMode;
use NormCache\Enums\CacheOperation;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Planning\AggregateDependencyCollector;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\QueryInspection;

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

        return (int) $this->cacheScalar(
            ResultKind::Count,
            fn() => parent::count($columns),
            (array) $columns,
            fn(QueryBuilder $base) => $base->count($columns)
        );
    }

    public function sum($column): mixed
    {
        return $this->cacheScalar(
            ResultKind::Aggregate,
            fn() => parent::sum($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->sum($column)
        );
    }

    public function avg($column): mixed
    {
        return $this->cacheScalar(
            ResultKind::Aggregate,
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
            ResultKind::Aggregate,
            fn() => parent::min($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->min($column)
        );
    }

    public function max($column): mixed
    {
        return $this->cacheScalar(
            ResultKind::Aggregate,
            fn() => parent::max($column),
            (array) $column,
            fn(QueryBuilder $base) => $base->max($column)
        );
    }

    public function exists(): bool
    {
        return (bool) $this->cacheScalar(
            ResultKind::Aggregate,
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
            ResultKind::Pluck,
            fn() => parent::pluck($column, $key),
            $columns,
            fn(QueryBuilder $base) => $this->pluckFromPreparedBase($base, $column, $key)
        );
    }

    public function value($column): mixed
    {
        return $this->cacheScalar(
            ResultKind::Value,
            fn() => parent::value($column),
            (array) $column,
            fn(QueryBuilder $base) => $this->valueFromPreparedBase($base, $column)
        );
    }

    private function cacheScalar(
        ResultKind $kind,
        \Closure $fallback,
        array $columns = [],
        ?\Closure $compute = null,
    ): mixed {
        if (ProjectionClassifier::hasCalculatedColumns($columns)) {
            return $fallback();
        }

        if (($kind === ResultKind::Pluck || $kind === ResultKind::Value) && $this->hasAfterQueryCallbacks()) {
            return $fallback();
        }

        $prepared = $this->prepareCacheExecution();
        $executionBuilder = $prepared->builder;
        $base = $prepared->base;
        $computeValue = $compute === null
            ? $fallback
            : fn() => $compute($base);
        $inferredDependencies = (new AggregateDependencyCollector)->collect($executionBuilder)->dependencies;

        if ($this->isSimpleScalarQuery($base, $inferredDependencies)) {
            $plan = CachePlan::result(
                operation: CacheOperation::Scalar,
                dependencies: DependencySet::singleModel($this->model::class),
            );
        } else {
            $plan = $executionBuilder->cachePlan($base, CachePlanContext::scalar(
                $kind->value,
                $columns,
                $inferredDependencies,
            ));

            if ($plan->mode === CacheMode::Bypass) {
                if (!$plan->hasBypassReason('opted_out')) {
                    CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons);
                }

                return $computeValue();
            }
        }

        $result = NormCache::result()->execute(
            $prepared,
            $plan,
            $kind,
            $columns,
            $computeValue
        );

        return $result[0];
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

        return ModelHydrator::transformScalars($results, $this->model, $column);
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

        return ModelHydrator::transformScalar($value, $this->model, $column);
    }

    private function isSimpleScalarQuery(QueryBuilder $base, DependencySet $inferredDependencies): bool
    {
        if (!$inferredDependencies->safe
            || $inferredDependencies->models !== []
            || $inferredDependencies->tables !== []
            || $this->isCacheSkipped()
            || !NormCache::isEnabled()
            || $this->getModel()->getConnection()->transactionLevel() > 0
            || $this->explicitDependencies() !== null
            || $this->explicitTableDependencies() !== []
        ) {
            return false;
        }

        $flags = $this->analyzer()->flags(
            $base,
            $this->getModel()->getTable(),
            null,
        );

        $bypassFlags = QueryInspection::RAW_ORDER
            | QueryInspection::RAW_WHERE
            | QueryInspection::SUBQUERY_WHERE
            | QueryInspection::LOCK
            | QueryInspection::NON_CANONICAL_FROM
            | QueryInspection::JOIN
            | QueryInspection::UNION;

        return ($flags & $bypassFlags) === 0;
    }
}
