<?php

namespace NormCache\Values;

final readonly class ObservationRecord
{
    /**
     * @param  list<mixed>  $bindings
     * @param  list<string>  $primaryKeys
     */
    public function __construct(
        public string $outcome,
        public ?string $route = null,
        public ?string $tableHash = null,
        public ?string $queryHash = null,
        public ?string $reason = null,
        public ?string $sql = null,
        public array $bindings = [],
        public ?string $modelClass = null,
        public ?string $rowAction = null,
        public array $primaryKeys = [],
    ) {}
}
