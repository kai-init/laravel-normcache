<?php

namespace NormCache\Events;

final readonly class QueryCacheMiss
{
    public function __construct(
        public string $modelClass,
        public string $key,
    ) {}
}
