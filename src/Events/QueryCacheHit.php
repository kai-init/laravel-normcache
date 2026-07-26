<?php

namespace NormCache\Events;

final readonly class QueryCacheHit
{
    /** @param list<mixed> $bindings */
    public function __construct(
        public string $route,
        public string $queryHash,
        public string $tableHash,
        public string $sql,
        public array $bindings,
        public ?string $modelClass = null,
    ) {}
}
