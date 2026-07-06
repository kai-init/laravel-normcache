<?php

namespace NormCache\Values;

use NormCache\Enums\CacheStatus;

final readonly class PivotCacheResult
{
    /**
     * @param  array<int|string, mixed>  $data  parentId => deserialized payload or null on miss
     */
    public function __construct(
        public string $seg,
        public array $data,
        public BuildHandle $build = new BuildHandle,
        public CacheStatus $status = CacheStatus::Hit,
    ) {}

    public function missedIds(): array
    {
        return array_keys(array_filter(
            $this->data,
            fn($payload) => !is_array($payload)
        ));
    }
}
