<?php

namespace DebugBar\DataCollector;

abstract class TimeDataCollector
{
    /** @param array<string, mixed> $params */
    public function addMeasure(string $label, float $start, float $end, array $params = []): void {}

    /** @return array<string, mixed> */
    public function collect(): array
    {
        return ['measures' => []];
    }
}
