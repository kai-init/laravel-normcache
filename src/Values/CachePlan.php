<?php

namespace NormCache\Values;

use NormCache\Enums\CacheMode;
use NormCache\Enums\CacheOperation;
use NormCache\Enums\CacheStrategy;

final readonly class CachePlan
{
    /**
     * @param  list<string>  $reasons
     * @param  array<string, list<string>>  $bypassReasons
     */
    public function __construct(
        public CacheMode $mode,
        public CacheStrategy $strategy,
        public CacheOperation $operation,
        public DependencySet $dependencies,
        public bool $normalizable = false,
        public ?array $columns = null,
        public ?array $primaryKeys = null,
        public array $reasons = [],
        public array $bypassReasons = [],
    ) {}

    public static function normalized(
        CacheOperation $operation,
        DependencySet $dependencies,
        ?array $columns = null,
        ?array $primaryKeys = null,
    ): self {
        return new self(
            mode: CacheMode::Normalized,
            strategy: CacheStrategy::NormalizedQuery,
            operation: $operation,
            dependencies: $dependencies,
            normalizable: true,
            columns: $columns,
            primaryKeys: $primaryKeys,
        );
    }

    public static function result(
        CacheOperation $operation,
        DependencySet $dependencies,
        bool $normalizable = false,
        ?array $columns = null,
        ?array $primaryKeys = null,
    ): self {
        return new self(
            mode: CacheMode::Result,
            strategy: CacheStrategy::VersionedResult,
            operation: $operation,
            dependencies: $dependencies,
            normalizable: $normalizable,
            columns: $columns,
            primaryKeys: $primaryKeys,
        );
    }

    public static function bypass(
        CacheOperation $operation,
        DependencySet $dependencies,
        array $reasons = [],
        array $bypassReasons = [],
    ): self {
        return new self(
            mode: CacheMode::Bypass,
            strategy: CacheStrategy::LiveQuery,
            operation: $operation,
            dependencies: $dependencies,
            reasons: $reasons,
            bypassReasons: $bypassReasons,
        );
    }

    public static function direct(
        CacheOperation $operation,
        DependencySet $dependencies,
        array $primaryKeys,
    ): self {
        return new self(
            mode: CacheMode::Normalized,
            strategy: CacheStrategy::DirectModels,
            operation: $operation,
            dependencies: $dependencies,
            normalizable: true,
            primaryKeys: $primaryKeys,
        );
    }

    public function hasBypassReason(string $category): bool
    {
        return isset($this->bypassReasons[$category]) && !empty($this->bypassReasons[$category]);
    }

    public function isCacheable(): bool
    {
        return $this->mode !== CacheMode::Bypass;
    }

    public function usesResultCache(): bool
    {
        return $this->mode === CacheMode::Result;
    }
}
