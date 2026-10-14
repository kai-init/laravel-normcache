<?php

namespace NormCache\Values;

final readonly class LoadedCanonical
{
    /**
     * @param  list<\stdClass>  $rows
     * @param  array<string, true>  $reusedTokens
     */
    public function __construct(
        public array $rows,
        public array $reusedTokens,
    ) {}
}
