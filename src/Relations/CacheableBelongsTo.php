<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\BelongsTo;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlanContext;
use NormCache\Planning\QueryAnalyzer;

class CacheableBelongsTo extends BelongsTo
{
    private array $eagerKeys = [];

    public function addEagerConstraints(array $models): void
    {
        parent::addEagerConstraints($models);

        $this->eagerKeys = $this->getEagerModelKeys($models);
    }

    public function getEager()
    {
        if (!$this->shouldUseCacheForEagerLoad()) {
            return parent::getEager();
        }

        $columns = QueryAnalyzer::resolveSelectedColumns($this->query->toBase(), null);

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection(
                NormCache::getModels($this->eagerKeys, $this->related::class, $columns, null, $this->query, false)
            )
        );
    }

    private function shouldUseCacheForEagerLoad(): bool
    {
        if ($this->eagerKeys === []
            || !$this->query instanceof CacheableBuilder
            || $this->getOwnerKeyName() !== $this->related->getKeyName()
            || $this->query->getEagerLoads() !== []) {
            return false;
        }

        $base = $this->query->toBase();
        $plan = $this->query->cachePlan($base, CachePlanContext::belongsToEagerLoad());

        return $plan->mode === CacheMode::Normalized;
    }
}
