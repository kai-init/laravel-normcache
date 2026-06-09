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
            foreach ($columns as $col) {
                if (!is_string($col)) {
                    return false;
                }
            }
            if (!isset(AttributeProjector::normalizeProjection($columns)[$ownerKey])) {
                return false;
            }
        }

        if ($this->isSimplePkEagerLoad($base, $builder)) {
            return true;
        }

        $plan = $builder->cachePlan($base, CachePlanContext::belongsToEagerLoad($columns ?? []));

        return $plan->mode === CacheMode::Normalized;
    }

    private function isSimplePkEagerLoad(QueryBuilder $base, CacheableBuilder $builder): bool
    {
        if ($builder->isCacheSkipped()
            || !NormCache::isEnabled()
            || $builder->getModel()->getConnection()->transactionLevel() > 0
            || !empty($base->joins)
            || !empty($base->orders)
            || !empty($base->groups)
            || !empty($base->havings)
            || !empty($base->unions)
            || $base->limit !== null
            || $base->offset > 0
            || $base->distinct
            || ($base->lock !== null && $base->lock !== false)
            || $builder->explicitDependencies() !== null
            || $builder->explicitTableDependencies() !== []) {
            return false;
        }

        $whereCount = count($base->wheres);

        if ($whereCount === 0 || $whereCount > 2) {
            return false;
        }

        // First where must be the In/InRaw FK constraint added by addEagerConstraints.
        $fkWhere = $base->wheres[0];
        if (($fkWhere['type'] ?? null) !== 'In' && ($fkWhere['type'] ?? null) !== 'InRaw') {
            return false;
        }
        if (($fkWhere['column'] ?? null) !== $this->getQualifiedOwnerKeyName()) {
            return false;
        }

        // Optional second where must be a soft-delete Null constraint.
        if ($whereCount === 2 && ($base->wheres[1]['type'] ?? null) !== 'Null') {
            return false;
        }

        return true;
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
