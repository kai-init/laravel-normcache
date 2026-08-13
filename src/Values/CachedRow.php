<?php

namespace NormCache\Values;

final readonly class CachedRow
{
    public function __construct(
        public string $generation,
        public ?string $epoch = null,
        public ?\stdClass $row = null,
        public ?string $reason = null,
    ) {}
}
