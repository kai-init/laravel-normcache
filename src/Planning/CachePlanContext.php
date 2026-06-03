<?php

namespace NormCache\Planning;

use NormCache\Enums\CacheOperation;

final readonly class CachePlanContext
{
    public function __construct(
        public CacheOperation $operation,
        public ?array $columns = null,
        public DependencySet $inferredDependencies = new DependencySet,
        public array $contextReasons = [],
        public ?string $kind = null,
    ) {}

    public static function models(?array $columns = null, DependencySet $inferred = new DependencySet): self
    {
        return new self(CacheOperation::Models, $columns, $inferred);
    }

    public static function scalar(string $kind, array $columns = [], DependencySet $inferred = new DependencySet): self
    {
        return new self(CacheOperation::Scalar, $columns, $inferred, kind: $kind);
    }

    public static function paginationCount(DependencySet $inferred = new DependencySet): self
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

    public static function pivot(array $columns = [], DependencySet $inferred = new DependencySet): self
    {
        return new self(CacheOperation::Pivot, $columns, $inferred);
    }

    public static function through(array $columns = [], DependencySet $inferred = new DependencySet): self
    {
        return new self(CacheOperation::Through, $columns, $inferred);
    }

    public function requiresNormalization(): bool
    {
        return $this->operation === CacheOperation::Models
            || $this->operation === CacheOperation::BelongsToEagerLoad
            || $this->operation === CacheOperation::MorphToEagerLoad;
    }
}
