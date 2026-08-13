<?php

namespace NormCache\Events;

final readonly class CacheInvalidated
{
    /** @param list<string> $primaryKeyTokens */
    public function __construct(
        public string $tableHash,
        public string $mode,
        public array $primaryKeyTokens = [],
    ) {}
}
