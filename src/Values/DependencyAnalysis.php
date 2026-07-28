<?php

namespace NormCache\Values;

final readonly class DependencyAnalysis
{
    /** @param list<TableIdentity> $tables */
    public function __construct(
        public array $tables,
        public bool $opaque = false,
        public bool $explicit = false,
        public bool $volatile = false,
    ) {}
}
