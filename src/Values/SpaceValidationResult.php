<?php

namespace NormCache\Values;

// Result of validating an operation's dependencies against one cache space.
// $dependenciesBySpace maps each dependency to the spaces it lives in, for explain().
final readonly class SpaceValidationResult
{
    /**
     * @param  list<string>  $invalidModels
     * @param  list<string>  $invalidTables
     * @param  array<string, list<string>>  $dependenciesBySpace
     */
    public function __construct(
        public bool $ok,
        public CacheSpace $space,
        public array $invalidModels = [],
        public array $invalidTables = [],
        public array $dependenciesBySpace = [],
    ) {}
}
