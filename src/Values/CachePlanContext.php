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
        public ?string $kind = null,
    ) {
        $this->inferredDependencies = $inferredDependencies ?? DependencySet::empty();
    }

    public static function models(?array $columns = null, ?DependencySet $inferred = null): self
    {
        return new self(CacheOperation::Models, $columns, $inferred);
    }

    public static function scalar(string $kind, array $columns = [], ?DependencySet $inferred = null, array $contextReasons = []): self
    {
        return new self(CacheOperation::Scalar, $columns, $inferred, $contextReasons, $kind);
    }

    public static function paginationCount(?DependencySet $inferred = null): self
    {
        return new self(CacheOperation::PaginationCount, null, $inferred, kind: 'pagination_count');
    }

    public static function belongsToEagerLoad(array $columns = []): self
    {
        return new self(CacheOperation::BelongsToEagerLoad, $columns);
    }

    public static function morphToEagerLoad(string $type): self
    {
        return new self(CacheOperation::MorphToEagerLoad, kind: $type);
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
