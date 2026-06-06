<?php

namespace NormCache\Planning;

final readonly class QueryAnalysis
{
    public function __construct(
        public ?array $selectedColumns = null,
        public ?array $primaryKeys = null,
        public array $bypassReasons = [],
        public ?array $tables = null,
    ) {}

    public function dependencyReasons(): array
    {
        return $this->bypassReasons['dependency'] ?? [];
    }

    public function normalizationReasons(): array
    {
        return $this->bypassReasons['normalization'] ?? [];
    }

    public function safetyReasons(): array
    {
        return $this->bypassReasons['safety'] ?? [];
    }

    public function optedOutReasons(): array
    {
        return $this->bypassReasons['opted_out'] ?? [];
    }

    public function hasOptedOutBypass(): bool
    {
        return $this->optedOutReasons() !== [];
    }

    public function hasSafetyBypass(): bool
    {
        return $this->safetyReasons() !== [];
    }

    public function hasDependencyBypass(): bool
    {
        return $this->dependencyReasons() !== [];
    }

    public function hasNormalizationBypass(): bool
    {
        return $this->normalizationReasons() !== [];
    }
}
