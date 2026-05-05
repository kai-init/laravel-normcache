<?php

namespace NormCache\Events;

final readonly class ModelCacheMiss
{
    public function __construct(
        public string $modelClass,
        public array $ids,
    ) {}
}
