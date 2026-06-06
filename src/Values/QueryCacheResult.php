<?php

namespace NormCache\Values;

use NormCache\Enums\CacheStatus;

final readonly class QueryCacheResult
{
    public function __construct(
        public CacheStatus $status,
        public ?string $key,
        public ?array $ids,
        public ?array $models,
        public ?string $buildingKey,
        public ?string $buildingToken,
        public array $versionKeys,
        public array $expectedVersions,
    ) {}
}
