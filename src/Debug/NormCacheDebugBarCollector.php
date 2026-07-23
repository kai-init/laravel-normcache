<?php

namespace NormCache\Debug;

use DebugBar\DataCollector\TimeDataCollector;
use NormCache\Planning\BypassReasons;

/**
 * DebugBar data collector for NormCache.
 * Only instantiated when normcache.debugbar is enabled and php-debugbar is installed.
 * Never imported outside of CacheServiceProvider.
 */
class NormCacheDebugBarCollector extends TimeDataCollector
{
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

    public function addQueryMeasure(string $type, string $modelClass, string $key, ?float $startTime, array $meta): void
    {
        $details = ['key' => $key];
        $contains = $meta['contains'] ?? $this->queryContains($type);

        if ($contains !== null) {
            $details['contains'] = $contains;
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

    public function addModelMeasure(string $type, string $modelClass, array $ids, ?float $startTime, array $meta): void
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

    public function addMetricMeasure(
        string $metric,
        int|float $value,
        string $modelClass,
        array $meta,
    ): void {
        $now = microtime(true);
        $this->addMeasure(
            '[metric] ' . $metric . ': ' . class_basename($modelClass),
            $now,
            $now,
            ['metric' => $metric, 'value' => $value, ...$meta],
        );
    }

    public function addInvalidationMeasure(
        string $dependencyType,
        string $target,
        int $count,
        array $spaces,
    ): void {
        $now = microtime(true);
        $this->addMeasure(
            '[invalidation] ' . $dependencyType . ': ' . $target,
            $now,
            $now,
            [
                'cache_kind' => 'version',
                'dependency_type' => $dependencyType,
                'count' => $count,
                'cache_spaces' => $spaces,
            ],
        );
    }

    public function addBypassMeasure(string $modelClass, array $groupedReasons, ?float $startTime): void
    {
        $labels = BypassReasons::labels();
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
        $shape = $meta['payload_shape'] ?? $meta['result_kind'] ?? null;

        return match (true) {
            is_string($shape) => $shape,
            str_starts_with($type, 'pivot ') => 'pivot',
            str_starts_with($type, 'through ') => 'through',
            isset($meta['cache_kind']) && is_string($meta['cache_kind']) => $meta['cache_kind'],
            default => $type,
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
