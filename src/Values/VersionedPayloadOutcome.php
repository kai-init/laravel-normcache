<?php

namespace NormCache\Values;

use NormCache\Enums\CacheStatus;

final readonly class VersionedPayloadOutcome
{
    public function __construct(
        public mixed $payload,
        public CacheStatus $status,
        public string $key,
        public BuildHandle $build,
    ) {}
}
