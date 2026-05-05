<?php

namespace NormCache\Events;

final readonly class ModelCacheHit
{
    public function __construct(
        public string $modelClass,
        public array $ids,
    ) {}
}
