<?php

namespace NormCache\Values;

use NormCache\Enums\CacheOperation;

final readonly class CachePlanContext
{
    public DependencySet $inferredDependencies;

    public function __construct(
        public CacheOperation $operation,
        public ?array $columns = null,
        ?DependencySet $inferredDependencies = null,
        public array $contextReasons = [],
        public bool $selectAll = false,
    ) {
        $this->inferredDependencies = $inferredDependencies ?? DependencySet::empty();
    }

    /** @param  bool  $selectAll  the caller requested the default ['*'] projection */
    public static function models(?array $columns = null, ?DependencySet $inferred = null, bool $selectAll = false): self
    {
        return new self(CacheOperation::Models, $columns, $inferred, selectAll: $selectAll);
    }

    public static function scalar(array $columns = [], ?DependencySet $inferred = null, array $contextReasons = []): self
    {
        return new self(CacheOperation::Scalar, $columns, $inferred, $contextReasons);
    }

    public static function paginationCount(?DependencySet $inferred = null): self
    {
        return new self(CacheOperation::PaginationCount, null, $inferred);
    }

    public static function belongsToEagerLoad(array $columns = []): self
    {
        return new self(CacheOperation::BelongsToEagerLoad, $columns);
    }

    public static function morphToEagerLoad(): self
    {
        return new self(CacheOperation::MorphToEagerLoad);
    }

    public static function pivot(array $columns = [], ?DependencySet $inferred = null): self
    {
        return new self(CacheOperation::Pivot, $columns, $inferred);
    }

    public static function through(array $columns = [], ?DependencySet $inferred = null): self
    {
        return new self(CacheOperation::Through, $columns, $inferred);
    }
}
