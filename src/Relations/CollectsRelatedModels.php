<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation;
use NormCache\Values\PreparedQuery;

/**
 * @mixin Relation
 *
 * @property Model $related
 */
trait CollectsRelatedModels
{
    /**
     * Hydrate the related models from a prepared (cache-bypassed) query.
     *
     * @param  ?\Closure(array): void  $beforeEagerLoad  Hook for relation-specific
     *                                                   per-model setup (e.g. pivot hydration) before eager loads run.
     */
    private function collectFromPreparedBuilder(
        PreparedQuery $prepared,
        bool $applyAfterCallbacks = true,
        ?\Closure $beforeEagerLoad = null,
    ): Collection {
        $builder = $prepared->builder;
        $models = $builder->getModels();

        if ($beforeEagerLoad !== null) {
            $beforeEagerLoad($models);
        }

        if (count($models) > 0) {
            $models = $builder->eagerLoadRelations($models);
        }

        $collection = $this->related->newCollection($models);

        return $applyAfterCallbacks
            ? $prepared->applyAfterCallbacks($collection)
            : $collection;
    }
}
