<?php

namespace NormCache\Debug;

use DebugBar\DataCollector\TimeDataCollector;
use NormCache\Values\ObservationRecord;

final class DebugBarCollector extends TimeDataCollector
{
    public function record(ObservationRecord $record): void
    {
        $detail = $record->reason ?? $record->route ?? $record->invalidationMode;
        $label = '[' . $record->outcome . ']';

        if ($record->modelClass !== null) {
            $label .= ' ' . class_basename($record->modelClass);
        }

        if ($detail !== null) {
            $label .= ($record->modelClass === null ? ' ' : ': ') . $detail;
        }

        $parameters = [
            'route' => $record->route,
            'table_hash' => $record->tableHash,
            'query_hash' => $record->queryHash,
            'reason' => $record->reason,
            'sql' => $record->sql,
            'bindings' => $record->bindings,
            'model_class' => $record->modelClass,
        ];

        if ($record->outcome === 'invalidation') {
            $parameters['invalidation_mode'] = $record->invalidationMode;
            $parameters['primary_key_tokens'] = $record->primaryKeyTokens;
        }

        $this->addMeasure($label, $record->startedAt, $record->endedAt, $parameters);
    }

    public function getName(): string
    {
        return 'normcache';
    }

    public function collect(): array
    {
        $data = parent::collect();
        $measures = is_array($data['measures'] ?? null) ? $data['measures'] : [];
        $elapsed = 0.0;

        foreach ($measures as $measure) {
            $elapsed += (float) ($measure['duration'] ?? 0.0);
        }

        $data['summary'] = self::summary(count($measures), $elapsed * 1000);

        return $data;
    }

    private static function summary(int $operations, float $milliseconds): string
    {
        return $operations . ' ops / ' . number_format($milliseconds, 1) . ' ms';
    }

    public function getWidgets(): array
    {
        return [
            'NormCache' => [
                'icon' => 'archive',
                'widget' => 'PhpDebugBar.Widgets.TimelineWidget',
                'map' => 'normcache',
                'default' => '{}',
            ],
            'NormCache:badge' => [
                'map' => 'normcache.summary',
                // Quoted twice: Debugbar injects defaults into JavaScript verbatim.
                'default' => "'0 ops / 0.0 ms'",
            ],
        ];
    }
}
