<?php

namespace NormCache\Debug;

use DebugBar\DataCollector\MessagesCollector;
use NormCache\Support\QueryInspector;

class NormCacheCollector extends MessagesCollector
{
    public function getName(): string
    {
        return 'normcache';
    }

    public function addQueryHit(string $modelClass, string $key): void
    {
        $this->addMessage('[Q HIT] ' . class_basename($modelClass) . " — {$key}", 'success');
    }

    public function addQueryMiss(string $modelClass, string $key): void
    {
        $this->addMessage('[Q MISS] ' . class_basename($modelClass) . " — {$key}", 'warning');
    }

    public function addBypassed(string $modelClass, array $grouped): void
    {
        $labels = QueryInspector::categoryLabels();
        $label = class_basename($modelClass);
        $lines = [];
        foreach ($grouped as $category => $reasons) {
            $lines[] = '  [' . ($labels[$category] ?? $category) . '] ' . implode(', ', $reasons);
        }
        $this->addMessage("[BYPASS] {$label}\n" . implode("\n", $lines), 'error');
    }

    public function addModelHit(string $modelClass, array $ids): void
    {
        $this->addMessage('[M HIT] ' . class_basename($modelClass) . ' — ids: ' . implode(', ', $ids), 'info');
    }

    public function addModelMiss(string $modelClass, array $ids): void
    {
        $this->addMessage('[M MISS] ' . class_basename($modelClass) . ' — ids: ' . implode(', ', $ids), 'warning');
    }
}
