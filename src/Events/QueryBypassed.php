<?php

namespace NormCache\Events;

final readonly class QueryBypassed
{
    /**
     * @param  array<string, list<string>>  $reasons  Bypass reasons grouped by category.
     *                                                Categories: 'dependency', 'normalization', 'safety', 'space', 'opted_out'
     */
    public function __construct(
        public string $modelClass,
        public array $reasons,
    ) {}
}
