<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheOperation;
use NormCache\Facades\NormCache;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Support\QueryHasher;
use NormCache\Support\RawAttributes;
use NormCache\Support\RelationCacheGuards;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\PreparedQuery;
use NormCache\Values\QueryInspection;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!$this->query instanceof CacheableBuilder) {
            return parent::get($columns);
        }

        $query = $this->query;
        $this->applyOneOfManyDependency($query);

        $debugbarStart = CacheReporter::beginMeasure();

        $prepared = $query->prepareScopedQuery();
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

        $hash = QueryHasher::forResultQuery($builder, $base);
        $relatedClass = $this->related::class;
        $throughClass = $this->throughParent::class;
        $depClasses = array_values(array_unique([
            $throughClass,
            ...$plan->dependencies->depClassesFor($relatedClass),
        ]));
        $depTableKeys = $plan->dependencies->tables;
        $tag = $builder->getCacheTag();
        $ttl = $builder->getQueryTtl();

        $runThrough = fn() => CacheFallback::rescue(
            NormCache::config(),
            fn() => NormCache::engine()->runNormalized(
                fetch: fn() => NormCache::through()->fetch($relatedClass, $hash, $tag, $depClasses, $depTableKeys),
                waitForBuild: fn() => NormCache::through()->waitForBuild(
                    $relatedClass, $hash, $tag, $depClasses, $depTableKeys
                ),
                onBuild: fn() => $this->getFromPreparedBuilder($prepared),
                onMiss: function ($result) use ($relatedClass, $throughClass, $shouldCacheModels, $debugbarStart, $prepared, $ttl) {
                    CacheReporter::queryMiss($relatedClass, $result->key, $debugbarStart,
                        ['through' => $throughClass], 'through miss');

                    $rawModels = $this->getFromPreparedBuilder($prepared, false);
                    $modelAttrs = [];
                    if ($shouldCacheModels) {
                        foreach ($rawModels as $model) {
                            $attrs = $model->getRawOriginal();
                            unset($attrs['laravel_through_key']);
                            $modelAttrs[$model->getKey()] = $attrs;
                        }
                    }

                    $cachePayload = $this->cachePayloadFromResult($rawModels);
                    $space = NormCache::keys()->activeSpace();

                    CacheFallback::attempt(
                        NormCache::config(),
                        function () use ($cachePayload, $result, $relatedClass, $ttl, $modelAttrs, $space) {
                            $stored = NormCache::through()->store(
                                $result->key,
                                $cachePayload['ids'],
                                $cachePayload['throughKeys'],
                                $ttl,
                                $result->build,
                            );

                            if ($stored) {
                                NormCache::models()->storeForBuild(
                                    $relatedClass,
                                    $modelAttrs,
                                    $result->build,
                                    $space,
                                );
                            }
                        },
                    );

                    return $prepared->applyAfterCallbacks($rawModels);
                },
                onHit: function ($result) use ($relatedClass, $prepared, $selectedColumns, $throughClass, $debugbarStart) {
                    CacheReporter::queryHit($relatedClass, $result->key ?? '', $debugbarStart,
                        ['through' => $throughClass], 'through hit');

                    $throughKeys = [];
                    foreach ($result->ids as $i => $id) {
                        $throughKeys[$id] = $result->throughKeys[$i] ?? null;
                    }

                    return $this->hydrateFromIds(
                        $result->ids, $relatedClass, $prepared, $selectedColumns, $throughKeys, $result->models
                    );
                },
            ),
            fn() => $this->getFromPreparedBuilder($prepared)
        );

        return NormCache::withSpace($plan->space, $runThrough);
    }

    private function applyOneOfManyDependency(CacheableBuilder $query): void
    {
        if (method_exists($this, 'isOneOfMany') && $this->isOneOfMany()) {
            $query->dependsOn([$this->throughParent::class]);
        }
    }

    private function shouldUseCache(CacheableBuilder $builder, Builder $base): ?CachePlan
    {
        if ($this->isSimpleThroughQuery($base, $builder)) {
            $space = NormCache::spaceFor($this->related::class, $builder->getSpace());
            $dependencies = DependencySet::singleModel($this->throughParent::class);
            $plan = CachePlan::result(
                operation: CacheOperation::Through,
                dependencies: $dependencies,
            )->withSpace($space);

            $plan = $builder->planner()->applySpaceValidation(
                $plan,
                $builder,
                $this->related,
            );

            return $plan->usesResultCache() ? $plan : null;
        }

        $projection = ProjectionClassifier::resolve($base, null);

        $plan = $builder->cachePlan($base, CachePlanContext::through(
            $projection ?? [],
            $builder->inferRelationDependencies(),
            DependencySet::singleModel($this->throughParent::class),
        ));

        return $plan->usesResultCache() ? $plan : null;
    }

    private function isSimpleThroughQuery(Builder $base, CacheableBuilder $builder): bool
    {
        if (RelationCacheGuards::blocksBypass($builder, $base)
            || count($base->joins ?? []) !== 1
            || ProjectionClassifier::hasCalculatedColumns($base->columns)) {
            return false;
        }

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
            $throughKeys[] = $model->getAttribute('laravel_through_key');
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
        ?array $raw = null,
    ): Collection {
        $builder = $prepared->builder;
        $models = NormCache::hydrator()->getModels($ids, $relatedClass, $selectedColumns, $raw, $builder, false, $this->related);

        if ($throughKeys !== []) {
            $getAttribute = RawAttributes::getAttributeClosure();
            $setAttribute = RawAttributes::setAttributeClosure();
            $keyName = $this->related->getKeyName();
            foreach ($models as $model) {
                $id = $getAttribute($model, $keyName);
                if (array_key_exists($id, $throughKeys)) {
                    $setAttribute($model, 'laravel_through_key', $throughKeys[$id]);
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
        return $prepared->collect(applyAfterCallbacks: $applyAfterCallbacks);
    }
}
