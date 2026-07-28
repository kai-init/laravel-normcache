<?php

namespace NormCache\Events;

final readonly class QueryBypassed
{
    /** @param list<mixed> $bindings */
    public function __construct(
        public string $reason,
        public string $sql,
        public array $bindings,
        public ?string $modelClass = null,
        public ?string $tableHash = null,
        public ?string $queryHash = null,
        public ?string $route = null,
    ) {}
}
