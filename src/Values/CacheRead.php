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
        public bool $overlayRejected = false,
        public ?MembershipPayload $staleMembership = null,
        public ?string $staleMembershipRaw = null,
    ) {}

    public function served(): bool
    {
        return $this->outcome->served();
    }

    public function promotable(): bool
    {
        return $this->served() && !$this->overlayRejected;
    }

    /** @param array<int, mixed> $rows */
    public function withRows(array $rows): self
    {
        return new self(
            $this->state,
            $this->outcome,
            $rows,
            $this->reason,
            $this->overlayRejected,
            $this->staleMembership,
            $this->staleMembershipRaw,
        );
    }

    public function withReason(?string $reason): self
    {
        return new self(
            $this->state,
            $this->outcome,
            $this->rows,
            $reason,
            $this->overlayRejected,
            $this->staleMembership,
            $this->staleMembershipRaw,
        );
    }

    public function asRepaired(?string $reason): self
    {
        return new self(
            $this->state,
            ReadOutcome::REPAIRED,
            $this->rows,
            $reason,
            $this->overlayRejected,
            $this->staleMembership,
            $this->staleMembershipRaw,
        );
    }
}
