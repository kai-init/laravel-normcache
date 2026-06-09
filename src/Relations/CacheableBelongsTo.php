<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheMode;
use NormCache\Facades\NormCache;
use NormCache\Support\AttributeProjector;
use NormCache\Support\ProjectionClassifier;
use NormCache\Values\CachePlanContext;
use NormCache\Values\PreparedQuery;

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
        if (!$this->query instanceof CacheableBuilder) {
            return parent::getEager();
        }

        $prepared = $this->query->prepareCacheExecution();
        $builder = $prepared->builder;
        $base = $prepared->base;
        $columns = ProjectionClassifier::resolve($base, null);

        if (!$this->shouldUseCacheForEagerLoad($columns, $base, $builder)) {
            return $this->getFromPreparedBuilder($prepared);
        }

        return $prepared->applyAfterCallbacks(
            $this->related->newCollection(
                NormCache::getModels($this->eagerKeys, $this->related::class, $columns, null, $builder, false)
            )
        );
    }

    private function shouldUseCacheForEagerLoad(
        ?array $columns,
        QueryBuilder $base,
        CacheableBuilder $builder,
    ): bool {
        $ownerKey = $this->getOwnerKeyName();

        if ($this->eagerKeys === []
            || $ownerKey !== $this->related->getKeyName()
            || $builder->getEagerLoads() !== []) {
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

        $plan = $builder->cachePlan($base, CachePlanContext::belongsToEagerLoad($columns ?? []));

        return $plan->mode === CacheMode::Normalized;
    }

    private function getFromPreparedBuilder(PreparedQuery $prepared): Collection
    {
        if ($this->eagerKeys === []) {
            return $this->related->newCollection();
        }

        $builder = $prepared->builder;
        $models = $builder->getModels();

        if (count($models) > 0) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $prepared->applyAfterCallbacks(
            $this->related->newCollection($models)
        );
    }
}
