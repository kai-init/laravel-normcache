<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheOperation;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\RelationCacheGuards;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\PreparedQuery;
use NormCache\Values\QueryInspection;

trait CachesOneOrManyThrough
{
    use CollectsRelatedModels;

    public function get($columns = ['*']): Collection
    {
        if (!$this->query instanceof CacheableBuilder) {
            return parent::get($columns);
        }

        $prepared = $this->query->prepareScopedQuery();
        $builder = $prepared->builder;
        $base = $prepared->base;
        $builder->addSelect(
            $this->shouldSelect($base->columns ? [] : $columns)
        );
        $prepared->applyBeforeCallbacks();

        $classification = ProjectionClassifier::classifyForRelation(
            $base,
            (array) $columns,
            $this->related->getTable(),
            $this->related->getKeyName()
        );

        $shouldCacheModels = $classification['shouldCacheRelatedModels'];
        $selectedColumns = $classification['selectedRelatedColumns'];

        $plan = $this->shouldUseCache($builder, $base);

        if ($plan === null || (!$shouldCacheModels && !$classification['relatedKeyInProjection'])) {
            return $this->getFromPreparedBuilder($prepared);
        }

        $relatedClass = $this->related::class;
        $throughClass = $this->throughParent::class;

        $plan = CachePlan::result(
            operation: $plan->operation,
            dependencies: $plan->dependencies->merge(DependencySet::singleModel($throughClass)),
            normalizable: $plan->normalizable,
            columns: $plan->columns,
            primaryKeys: $plan->primaryKeys,
        );

        $liveResult = null;

        [$payload, $cached] = NormCache::result()->execute(
            $prepared,
            $plan,
            ResultKind::Through,
            [],
            function () use ($prepared, $shouldCacheModels, $relatedClass, &$liveResult) {
                $rawModels = $this->getFromPreparedBuilder($prepared, false);

                if ($shouldCacheModels) {
                    $modelAttrs = [];
                    foreach ($rawModels as $model) {
                        $attrs = $model->getRawOriginal();
                        unset($attrs['laravel_through_key']);
                        $modelAttrs[$model->getKey()] = $attrs;
                    }
                    NormCache::attempt(fn() => NormCache::cacheModelAttrs($relatedClass, $modelAttrs));
                }

                $liveResult = $rawModels;

                return $this->cachePayloadFromResult($rawModels);
            }
        );

        if (!$cached) {
            return $prepared->applyAfterCallbacks($liveResult);
        }

        return $this->hydrateFromIds($payload['ids'], $relatedClass, $prepared, $selectedColumns, $payload['throughKeys'] ?? []);
    }

    private function shouldUseCache(CacheableBuilder $builder, Builder $base): ?CachePlan
    {
        if ($this->isSimpleThroughQuery($base, $builder)) {
            return CachePlan::result(
                operation: CacheOperation::Through,
                dependencies: new DependencySet(models: [$this->throughParent::class]),
            );
        }

        $projection = ProjectionClassifier::resolve($base, null);

        $plan = $builder->cachePlan($base, CachePlanContext::through(
            $projection ?? [],
            $builder->inferAggregateDependencies()
        ));

        return $plan->usesResultCache() ? $plan : null;
    }

    private function isSimpleThroughQuery(Builder $base, CacheableBuilder $builder): bool
    {
        // Standard HasManyThrough/HasOneThrough always has exactly one join (the intermediate table)
        // The planner grants a bypass relaxation for this shape; validate it directly here
        if (RelationCacheGuards::blocksBypass($builder, $base)
            || count($base->joins ?? []) !== 1
            || ProjectionClassifier::hasCalculatedColumns($base->columns)) {
            return false;
        }

        // The intermediate JOIN is the only relaxation this fast path is allowed to make.
        // Compare against the canonical related table so NON_CANONICAL_FROM is detectable.
        $flags = (new QueryAnalyzer)->flags($base, $this->related->getTable(), $base->columns);

        return ($flags & ~QueryInspection::JOIN) === 0;
    }

    private function cachePayloadFromResult(Collection $result): array
    {
        $ids = [];
        $throughKeys = [];

        foreach ($result as $model) {
            $id = $model->getKey();
            $ids[] = $id;
            $throughKeys[$id] = $model->getAttribute('laravel_through_key');
        }

        return [
            'ids' => $ids,
            'throughKeys' => $throughKeys,
        ];
    }

    private function hydrateFromIds(
        array $ids,
        string $relatedClass,
        PreparedQuery $prepared,
        ?array $selectedColumns,
        array $throughKeys = [],
    ): Collection {
        $builder = $prepared->builder;
        $models = NormCache::getModels($ids, $relatedClass, $selectedColumns, null, $builder, false, $this->related);

        if ($throughKeys !== []) {
            foreach ($models as $model) {
                $id = $model->getKey();
                if (array_key_exists($id, $throughKeys)) {
                    $model->setAttribute('laravel_through_key', $throughKeys[$id]);
                }
            }
        }

        if ($models && $builder->getEagerLoads()) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $prepared->applyAfterCallbacks($this->related->newCollection($models));
    }

    private function getFromPreparedBuilder(PreparedQuery $prepared, bool $applyAfterCallbacks = true): Collection
    {
        return $this->collectFromPreparedBuilder($prepared, $applyAfterCallbacks);
    }
}
