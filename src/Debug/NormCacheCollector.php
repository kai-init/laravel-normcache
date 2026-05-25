<?php

namespace NormCache\Debug;

use DebugBar\DataCollector\TimeDataCollector;
use NormCache\Support\QueryInspector;

class NormCacheCollector extends TimeDataCollector
{
    private static ?self $instance = null;

    public static function register(self $collector): void
    {
        self::$instance = $collector;
    }

    public static function beginMeasure(): ?float
    {
        return self::$instance !== null ? microtime(true) : null;
    }

    public static function recordQuery(string $type, string $modelClass, string $key, ?float $startTime, array $meta = []): void
    {
        self::$instance?->addQueryMeasure($type, $modelClass, $key, $startTime, $meta);
    }

    public static function recordModel(string $type, string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        self::$instance?->addModelMeasure($type, $modelClass, $ids, $startTime, $meta);
    }

    public static function recordBypass(string $modelClass, array $groupedReasons, ?float $startTime): void
    {
        self::$instance?->addBypassMeasure($modelClass, $groupedReasons, $startTime);
    }

    public function getName(): string
    {
        return 'normcache';
    }

    public function collect(): array
    {
        $data = parent::collect();
        $count = count($data['measures'] ?? []);
        $totalMs = array_sum(array_column($data['measures'] ?? [], 'duration')) * 1000;

        $data['normcache-measures'] = $count . ' / ' . number_format($totalMs, 2) . 'ms';

        return $data;
    }

    public function getWidgets(): array
    {
        return [
            'Normcache' => [
                'icon' => 'archive',
                'widget' => 'PhpDebugBar.Widgets.TimelineWidget',
                'map' => 'normcache',
                'default' => '{}',
            ],
            'Normcache:badge' => [
                'map' => 'normcache.normcache-measures',
                'default' => 0,
            ],
        ];
    }

    private function addQueryMeasure(string $type, string $modelClass, string $key, ?float $startTime, array $meta): void
    {
        $details = ['key' => $key];

        if (array_key_exists('kind', $meta)) {
            $details['kind'] = $meta['kind'];
        }

        $contains = $meta['contains'] ?? $this->queryContains($type);

        if ($contains !== null) {
            $details['contains'] = $contains;
        }

        if (array_key_exists('contains_model', $meta)) {
            $details['contains_model'] = $meta['contains_model'];
        }

        foreach ($meta as $field => $value) {
            if (!array_key_exists($field, $details)) {
                $details[$field] = $value;
            }
        }

        $this->addMeasure(
            '[' . $type . '] ' . class_basename($modelClass) . ': ' . $this->querySummary($type, $meta),
            $startTime ?? microtime(true),
            microtime(true),
            $details
        );
    }

    private function addModelMeasure(string $type, string $modelClass, array $ids, ?float $startTime, array $meta): void
    {
        $count = count($ids);
        $suffix = $count === 1 ? '1 id' : "{$count} ids";

        $this->addMeasure(
            '[' . $type . '] ' . class_basename($modelClass) . ": {$suffix}",
            $startTime ?? microtime(true),
            microtime(true),
            ['ids' => $ids, ...$meta]
        );
    }

    private function addBypassMeasure(string $modelClass, array $groupedReasons, ?float $startTime): void
    {
        $labels = QueryInspector::categoryLabels();
        $parts = [];

        foreach ($groupedReasons as $category => $items) {
            $parts[] = ($labels[$category] ?? $category) . ': ' . implode(', ', $items);
        }

        $this->addMeasure(
            '[bypass] ' . class_basename($modelClass),
            $startTime ?? microtime(true),
            microtime(true),
            ['reasons' => implode(' | ', $parts)]
        );
    }

    private function querySummary(string $type, array $meta): string
    {
        return match (true) {
            ($meta['kind'] ?? null) === 'count'        => 'count',
            ($meta['kind'] ?? null) === 'ids + models' => 'ids + models',
            ($meta['kind'] ?? null) === 'ids'          => 'ids',
            str_starts_with($type, 'pivot ')           => 'pivot',
            str_starts_with($type, 'through ')         => 'through',
            default                                    => $meta['kind'] ?? $type,
        };
    }

    private function queryContains(string $type): ?string
    {
        return match (true) {
            str_starts_with($type, 'query hit') => 'model payload fetch and deserialize',
            default => null,
        };
    }
}
