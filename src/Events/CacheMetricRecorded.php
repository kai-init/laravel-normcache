<?php

namespace NormCache\Events;

use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\ResultKind;

final readonly class CacheMetricRecorded
{
    public function __construct(
        public string $metric,
        public int|float $value,
        public CacheKind $cacheKind,
        public CacheStatus $status,
        public string $modelClass,
        public ?ResultKind $resultKind = null,
        public ?string $cacheSpace = null,
        public array $meta = [],
    ) {}
}
