<?php

namespace NormCache\Debug;

use DebugBar\DataCollector\TimeDataCollector;
use NormCache\Values\ObservationRecord;

final class DebugBarCollector extends TimeDataCollector
{
    public function record(ObservationRecord $record): void
    {
        $now = microtime(true);
        $label = '[normcache ' . $record->outcome . '] '
            . ($record->route ?? $record->invalidationMode ?? $record->reason ?? '');

        $this->addMeasure($label, $now, $now, [
            'route' => $record->route,
            'table_hash' => $record->tableHash,
            'query_hash' => $record->queryHash,
            'reason' => $record->reason,
            'sql' => $record->sql,
            'bindings' => $record->bindings,
            'model_class' => $record->modelClass,
            'invalidation_mode' => $record->invalidationMode,
            'primary_key_tokens' => $record->primaryKeyTokens,
        ]);
    }

    public function getName(): string
    {
        return 'normcache';
    }

    public function collect(): array
    {
        $data = parent::collect();
        $measures = $data['measures'] ?? [];
        $data['summary'] = count($measures) . ' operations';

        return $data;
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
                'default' => '0 operations',
            ],
        ];
    }
}
