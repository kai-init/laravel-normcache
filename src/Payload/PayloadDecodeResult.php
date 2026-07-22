<?php

namespace NormCache\Payload;

final readonly class PayloadDecodeResult
{
    private function __construct(
        public bool $valid,
        public mixed $payload,
        public bool $empty,
    ) {}

    public static function valid(mixed $payload, bool $empty = false): self
    {
        return new self(true, $payload, $empty);
    }

    public static function corrupt(): self
    {
        return new self(false, null, false);
    }
}
