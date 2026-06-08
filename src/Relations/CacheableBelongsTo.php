<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Support\AttributeProjector;
use NormCache\Support\ProjectionClassifier;
use NormCache\Values\CachePlanContext;

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
        $base = $this->query->toBase();
        $columns = ProjectionClassifier::resolve($base, null);

        if (!$this->shouldUseCacheForEagerLoad($columns, $base)) {
            return parent::getEager();
        }

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection(
                NormCache::getModels($this->eagerKeys, $this->related::class, $columns, null, $this->query, false)
            )
        );
    }

    private function shouldUseCacheForEagerLoad(?array $columns, QueryBuilder $base): bool
    {
        $ownerKey = $this->getOwnerKeyName();

        if ($this->eagerKeys === []
            || !$this->query instanceof CacheableBuilder
            || $ownerKey !== $this->related->getKeyName()
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
            if ($allStrings && !isset(AttributeProjector::normalizeProjection($columns)[$ownerKey])) {
                return false;
            }
        }

        $plan = $this->query->cachePlan($base, CachePlanContext::belongsToEagerLoad($columns ?? []));

        return $plan->mode === CacheMode::Normalized;
    }
}
