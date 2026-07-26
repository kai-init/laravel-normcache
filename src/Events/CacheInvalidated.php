<?php

namespace NormCache\Events;

final readonly class CacheInvalidated
{
    /** @param list<string> $primaryKeys */
    public function __construct(
        public string $tableHash,
        public string $action,
        public array $primaryKeys = [],
    ) {}
}
