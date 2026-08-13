<?php

namespace DebugBar\DataCollector;

abstract class TimeDataCollector
{
    /** @var list<array<string, mixed>> */
    protected array $measures = [];

    /** @param array<string, mixed> $params */
    public function addMeasure(string $label, float $start, float $end, array $params = []): void
    {
        $this->measures[] = [
            'label' => $label,
            'start' => $start,
            'end' => $end,
            'duration' => $end - $start,
            'params' => $params,
        ];
    }

    /** @return array<string, mixed> */
    public function collect(): array
    {
        return ['measures' => $this->measures];
    }
}
