<?php

namespace NormCache\Values;

final readonly class DependencyAnalysis
{
    /** @param list<TableIdentity> $tables */
    public function __construct(
        public ?TableIdentity $root,
        public array $tables,
        public bool $queryScoped,
        public ?string $bypassReason,
    ) {}
}
