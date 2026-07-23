<?php

namespace NormCache\Values;

use NormCache\Enums\CacheOperation;
use NormCache\Enums\CacheStrategy;

final readonly class CachePlan
{
    /**
     * @param  array<string, list<string>>  $bypassReasons
     */
    public function __construct(
        public CacheStrategy $strategy,
        public CacheOperation $operation,
        public DependencySet $dependencies,
        public ?array $columns = null,
        public ?array $primaryKeys = null,
        public array $bypassReasons = [],
        public ?CacheSpace $space = null,
    ) {}

    public function withSpace(CacheSpace $space): self
    {
        return new self(
            strategy: $this->strategy,
            operation: $this->operation,
            dependencies: $this->dependencies,
            columns: $this->columns,
            primaryKeys: $this->primaryKeys,
            bypassReasons: $this->bypassReasons,
            space: $space,
        );
    }

    public static function modelIndex(
        CacheOperation $operation,
        DependencySet $dependencies,
        ?array $columns = null,
        ?array $primaryKeys = null,
    ): self {
        return new self(
            strategy: CacheStrategy::ModelIndex,
            operation: $operation,
            dependencies: $dependencies,
            columns: $columns,
            primaryKeys: $primaryKeys,
        );
    }

    public static function result(
        CacheOperation $operation,
        DependencySet $dependencies,
        ?array $columns = null,
        ?array $primaryKeys = null,
    ): self {
        return new self(
            strategy: CacheStrategy::Result,
            operation: $operation,
            dependencies: $dependencies,
            columns: $columns,
            primaryKeys: $primaryKeys,
        );
    }

    public static function bypass(
        CacheOperation $operation,
        DependencySet $dependencies,
        array $bypassReasons = [],
    ): self {
        return new self(
            strategy: CacheStrategy::LiveQuery,
            operation: $operation,
            dependencies: $dependencies,
            bypassReasons: $bypassReasons,
        );
    }

    public static function direct(
        CacheOperation $operation,
        DependencySet $dependencies,
        array $primaryKeys,
        ?array $columns = null,
    ): self {
        return new self(
            strategy: CacheStrategy::DirectModels,
            operation: $operation,
            dependencies: $dependencies,
            columns: $columns,
            primaryKeys: $primaryKeys,
        );
    }

    public function hasBypassReason(string $category): bool
    {
        return isset($this->bypassReasons[$category]) && !empty($this->bypassReasons[$category]);
    }

    /** @return list<string> all bypass reasons, category order preserved */
    public function flatReasons(): array
    {
        return array_values(array_unique(array_merge(...array_values($this->bypassReasons ?: [[]]))));
    }

    public function isCacheable(): bool
    {
        return $this->strategy !== CacheStrategy::LiveQuery;
    }

    public function usesModelCache(): bool
    {
        return $this->strategy === CacheStrategy::ModelIndex
            || $this->strategy === CacheStrategy::DirectModels;
    }

    public function usesResultCache(): bool
    {
        return $this->strategy === CacheStrategy::Result;
    }
}
