<?php

namespace NormCache\Values;

use NormCache\Enums\CacheMode;
use NormCache\Enums\CacheOperation;

final readonly class CachePlan
{
    /**
     * @param  list<string>  $reasons
     * @param  array<string, list<string>>  $bypassReasons
     */
    public function __construct(
        public CacheMode $mode,
        public CacheOperation $operation,
        public DependencySet $dependencies,
        public bool $normalizable = false,
        public ?array $columns = null,
        public ?array $primaryKeys = null,
        public array $reasons = [],
        public array $bypassReasons = [],
    ) {}

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
