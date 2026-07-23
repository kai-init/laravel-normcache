<?php

namespace NormCache\Events;

final readonly class CacheInvalidated
{
    public function __construct(
        public string $dependencyType,
        public string $target,
        public int $count,
        public array $spaces = [],
    ) {}
}
