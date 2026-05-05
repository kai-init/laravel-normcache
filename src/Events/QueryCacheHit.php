<?php

namespace NormCache\Events;

final readonly class QueryCacheHit
{
    public function __construct(
        public string $modelClass,
        public string $key,
    ) {}
}
