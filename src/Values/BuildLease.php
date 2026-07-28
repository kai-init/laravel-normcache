<?php

namespace NormCache\Values;

final readonly class BuildLease
{
    public function __construct(
        public bool $owner,
        public string $buildingKey,
        public ?string $wakeKey,
        public ?string $token,
    ) {}
}
