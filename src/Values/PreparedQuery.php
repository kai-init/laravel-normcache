<?php

namespace NormCache\Values;

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

    public function applyAfterCallbacks(mixed $result): mixed
    {
        return $this->builder->applyAfterQueryCallbacks($result);
    }
}
