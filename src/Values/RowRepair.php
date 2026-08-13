<?php

namespace NormCache\Values;

use NormCache\Enums\ReadOutcome;

final readonly class RowRepair
{
    /** @param array<string, \stdClass> $rows */
    public function __construct(
        public array $rows,
        public ReadOutcome $outcome,
    ) {}
}
