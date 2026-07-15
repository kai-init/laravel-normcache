<?php

namespace NormCache\Values;

use NormCache\Enums\CacheOperation;

final readonly class CachePlanContext
{
    public DependencySet $requiredDependencies;

    public function __construct(
        public CacheOperation $operation,
        public ?array $columns = null,
        public array $contextReasons = [],
        public bool $selectAll = false,
        ?DependencySet $requiredDependencies = null,
    ) {
        $this->requiredDependencies = $requiredDependencies ?? DependencySet::empty();
    }

    /** @param  bool  $selectAll  the caller requested the default ['*'] projection */
    public static function models(?array $columns = null, bool $selectAll = false): self
    {
        return new self(CacheOperation::Models, $columns, selectAll: $selectAll);
    }

    public static function scalar(array $columns = [], array $contextReasons = []): self
    {
        return new self(CacheOperation::Scalar, $columns, $contextReasons);
    }

    public static function paginationCount(): self
    {
        return new self(CacheOperation::PaginationCount);
    }

    public static function pivot(array $columns = []): self
    {
        return new self(CacheOperation::Pivot, $columns);
    }

    public static function through(array $columns = [], ?DependencySet $required = null): self
    {
        return new self(
            CacheOperation::Through,
            $columns,
            requiredDependencies: $required,
        );
    }
}
