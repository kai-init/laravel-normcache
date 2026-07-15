<?php

namespace NormCache\Values;

// Result of validating an operation's dependencies against one cache space.
// $dependenciesBySpace is populated for explain() or failed validation only.
final readonly class SpaceValidationResult
{
    /**
     * @param  list<string>  $invalidModels
     * @param  array<string, list<string>>  $dependenciesBySpace
     */
    public function __construct(
        public bool $isValid,
        public CacheSpace $space,
        public array $invalidModels = [],
        public array $dependenciesBySpace = [],
    ) {}
}
