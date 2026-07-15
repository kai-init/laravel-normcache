<?php

namespace NormCache\Values;

use NormCache\Enums\CacheStatus;

final readonly class ResultCacheResult
{
    public function __construct(
        public CacheStatus $status,
        public ?string $key,
        public mixed $payload,
        public BuildHandle $build = new BuildHandle,
    ) {}
}
