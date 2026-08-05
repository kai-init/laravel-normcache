<?php

namespace NormCache\Values;

final readonly class DependencyAnalysis
{
    /** @param list<TableIdentity> $tables */
    public function __construct(
        public array $tables,
        public bool $opaque = false,
        public bool $explicit = false,
        public bool $unresolved = false,
    ) {}

    /** @param list<TableIdentity> $tables */
    public static function hasExternalTo(TableIdentity $root, array $tables): bool
    {
        foreach ($tables as $table) {
            if ($table->hash !== $root->hash) {
                return true;
            }
        }

        return false;
    }
}
