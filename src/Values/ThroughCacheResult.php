<?php

namespace NormCache\Values;

use NormCache\Enums\CacheStatus;

final readonly class ThroughCacheResult
{
    public function __construct(
        public CacheStatus $status,
        public ?string $key,
        public ?array $ids,
        public ?array $throughKeys,
        public ?array $models,
        public ?string $buildingKey,
        public ?string $buildingToken,
        public array $versionKeys,
        public array $expectedVersions,
        public ?string $wakeKey = null,
    ) {}
}
