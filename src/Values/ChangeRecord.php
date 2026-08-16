<?php

namespace NormCache\Values;

final readonly class ChangeRecord
{
    /** @param list<string> $columns */
    public function __construct(
        public bool $valid,
        public string $mutation = '',
        public array $columns = [],
        public bool $precise = false,
    ) {}

    public static function corrupt(): self
    {
        return new self(false);
    }
}
