<?php

namespace NormCache\Values;

final readonly class OverlayAdmission
{
    private function __construct(
        public ?string $payload,
        public bool $rejected,
    ) {}

    public static function accepted(string $payload): self
    {
        return new self($payload, false);
    }

    public static function rejected(): self
    {
        return new self(null, true);
    }

    public static function notAttempted(): self
    {
        return new self(null, false);
    }
}
