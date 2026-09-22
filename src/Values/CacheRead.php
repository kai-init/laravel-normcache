<?php

namespace NormCache\Values;

use NormCache\Enums\ReadOutcome;

final readonly class CacheRead
{
    /** @param array<int, mixed> $rows */
    public function __construct(
        public CacheState $state,
        public ReadOutcome $outcome,
        public array $rows = [],
        public ?string $reason = null,
    ) {}

    public function served(): bool
    {
        return $this->outcome->served();
    }

    public function withReason(?string $reason): self
    {
        return new self($this->state, $this->outcome, $this->rows, $reason);
    }
}
