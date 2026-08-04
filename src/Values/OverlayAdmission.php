<?php

namespace NormCache\Values;

final readonly class OverlayAdmission
{
    /** @param array{0: string, 1: string}|null $entry */
    private function __construct(
        public ?array $entry,
        public bool $rejected,
    ) {}

    public static function accepted(string $key, string $payload): self
    {
        return new self([$key, $payload], false);
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
