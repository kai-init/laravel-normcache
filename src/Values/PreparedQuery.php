<?php

namespace NormCache\Values;

use Closure;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;

final class PreparedQuery
{
    private bool $beforeCallbacksApplied = false;

    public function __construct(
        public readonly CacheableBuilder $builder,
        public readonly QueryBuilder $base,
    ) {}

    /** Clone of base with the caller's projection injected when the query itself selects none. */
    public function baseWithColumns(array $columns): QueryBuilder
    {
        $base = clone $this->base;

        if (empty($base->columns) && $columns !== ['*']) {
            $base->columns = $columns;
        }

        return $base;
    }

    public function applyBeforeCallbacks(): self
    {
        if (!$this->beforeCallbacksApplied) {
            $this->base->applyBeforeQueryCallbacks();
            $this->beforeCallbacksApplied = true;
        }

        return $this;
    }

    public function collect(
        array $columns = ['*'],
        bool $applyAfterCallbacks = true,
        ?Closure $beforeEagerLoad = null,
    ): Collection {
        $models = $this->builder->getModels($columns);
        $beforeEagerLoad?->__invoke($models);

        return $this->finalizeModels($models, $applyAfterCallbacks);
    }

    public function finalizeModels(array $models, bool $applyAfterCallbacks = true): Collection
    {
        if ($models !== [] && $this->builder->getEagerLoads() !== []) {
            $models = $this->builder->eagerLoadRelations($models);
        }

        $collection = $this->builder->getModel()->newCollection($models);

        return $applyAfterCallbacks
            ? $this->builder->applyAfterQueryCallbacks($collection)
            : $collection;
    }

    public function applyAfterCallbacks(mixed $result): mixed
    {
        return $this->builder->applyAfterQueryCallbacks($result);
    }
}
