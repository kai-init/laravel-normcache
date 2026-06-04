<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\BelongsTo;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlanContext;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Support\AttributeProjector;

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
        $columns = QueryAnalyzer::resolveSelectedColumns($this->query->toBase(), null);

        if (!$this->shouldUseCacheForEagerLoad($columns)) {
            return parent::getEager();
        }

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection(
                NormCache::getModels($this->eagerKeys, $this->related::class, $columns, null, $this->query, false)
            )
        );
    }

    private function shouldUseCacheForEagerLoad(?array $columns): bool
    {
        if ($this->eagerKeys === []
            || !$this->query instanceof CacheableBuilder
            || $this->getOwnerKeyName() !== $this->related->getKeyName()
            || $this->query->getEagerLoads() !== []) {
            return false;
        }

        if ($columns !== null) {
            $allStrings = true;
            foreach ($columns as $col) {
                if (!is_string($col)) {
                    $allStrings = false;
                    break;
                }
            }
            if ($allStrings && !isset(AttributeProjector::normalizeProjection($columns)[$this->getOwnerKeyName()])) {
                return false;
            }
        }

        $base = $this->query->toBase();
        $plan = $this->query->cachePlan($base, CachePlanContext::belongsToEagerLoad($columns ?? []));

        return $plan->mode === CacheMode::Normalized;
    }
}
