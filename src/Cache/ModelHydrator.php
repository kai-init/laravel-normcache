<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
use NormCache\Support\AttributeProjector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisStore;

final class ModelHydrator
{
    private static ?\Closure $hydrateClosure = null;

    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $ttl,
        private readonly bool $fireRetrieved,
    ) {}

    public function getModels(
        array $ids,
        string $modelClass,
        ?array $columns = null,
        ?array $raw = null,
        ?EloquentBuilder $missedQuery = null,
        bool $preserveQueryShape = true,
        ?Model $prototype = null,
    ): array {
        if ($ids === []) {
            return [];
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $containedInQueryHit = $raw !== null;

        // arrays may come with sparse numeric keys, for example after array_unique().
        $ids = array_values($ids);
        $classKey = $this->keys->classKey($modelClass);

        if ($raw === null) {
            $prefix = $this->keys->modelPrefix($classKey);
            $keys = [];
            foreach ($ids as $id) {
                $keys[] = $prefix . $id;
            }
            $raw = $this->store->getMany($keys);
        }

        $projection = $columns !== null ? AttributeProjector::normalizeProjection($columns) : null;
        ['hits' => $hits, 'missed' => $missed] = $this->hydrateModels($ids, $modelClass, $raw, $projection, $prototype);

        if ($hits !== []) {
            CacheReporter::modelHit($modelClass, array_keys($hits), $debugbarStart, [
                'suppress_collector' => $containedInQueryHit,
                'ids' => $ids,
            ]);
        }

        if ($missed === []) {
            return array_values($hits);
        }

        CacheReporter::modelMiss($modelClass, $missed, $debugbarStart, [
            'hits' => array_keys($hits),
            'partial' => $hits !== [],
        ]);

        $fetched = $this->fetchFromDatabaseAndCache(
            $missed,
            $modelClass,
            $classKey,
            $projection,
            $missedQuery,
            $preserveQueryShape,
        );

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($hits[$id]) || isset($fetched[$id])) {
                $ordered[] = $hits[$id] ?? $fetched[$id];
            }
        }

        return $ordered;
    }

    public function hydrateResult(array $payload, string|Model $model, bool $cached = true): array
    {
        $modelClass = $model instanceof Model ? $model::class : $model;
        $prototype = $model instanceof Model ? $model : CacheKeyBuilder::prototype($modelClass);
        $fire = $this->fireRetrieved;
        $hydrate = self::hydrateClosure();

        $models = [];
        foreach ($payload as $attrs) {
            $instance = clone $prototype;
            $hydrate($instance, $attrs, $fire);
            $models[] = $instance;
        }

        if ($cached && $models !== []) {
            $keys = [];
            foreach ($models as $m) {
                $key = $m->getKey();
                if ($key !== null) {
                    $keys[] = $key;
                }
            }
            if ($keys !== []) {
                CacheReporter::modelHit($modelClass, $keys, null);
            }
        }

        return $models;
    }

    private function hydrateModels(array $ids, string $modelClass, array $raw, ?array $projection, ?Model $prototype = null): array
    {
        $prototype = $prototype ?? CacheKeyBuilder::prototype($modelClass);
        $fire = $this->fireRetrieved;
        $hydrate = self::hydrateClosure();
        $hits = [];
        $missed = [];

        foreach ($ids as $i => $id) {
            $attrs = $raw[$i];

            if ($attrs === null || $attrs === false || !is_array($attrs)) {
                $missed[] = $id;

                continue;
            }

            if ($projection !== null) {
                $attrs = AttributeProjector::projectAttributes($attrs, $projection);
            }

            $instance = clone $prototype;
            $hydrate($instance, $attrs, $fire);
            $hits[$id] = $instance;
        }

        return ['hits' => $hits, 'missed' => $missed];
    }

    private function fetchFromDatabaseAndCache(
        array $missed,
        string $modelClass,
        string $classKey,
        ?array $projection,
        ?EloquentBuilder $missedQuery,
        bool $preserveQueryShape,
    ): array {
        $modelVersion = $this->versions->currentVersion($modelClass);
        $prototype = CacheKeyBuilder::prototype($modelClass);
        $pk = $prototype->getKeyName();
        $qualifiedPk = $prototype->getQualifiedKeyName();
        $query = $this->prepareMissedQuery(
            $modelClass,
            $missedQuery instanceof CacheableBuilder ? $missedQuery : null,
            $preserveQueryShape
        );
        $loaded = $query->whereIn($qualifiedPk, $missed)
            ->get(['*'])
            ->keyBy($pk);

        $attrsByKey = [];
        $deletedAtCol = CacheKeyBuilder::deletedAtColumn($modelClass);

        foreach ($loaded as $id => $model) {
            $attrs = $model->getRawOriginal();

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if (!$isTrashed) {
                $attrsByKey[$this->keys->modelPrefix($classKey) . $id] = $attrs;
            }

            if ($projection !== null) {
                $model->setRawAttributes(AttributeProjector::projectAttributes($attrs, $projection), true);
            }
        }

        if ($attrsByKey !== []) {
            $this->store->setManyTrackedIfVersion(
                $attrsByKey,
                $this->ttl,
                $this->keys->membersKey($classKey),
                $this->keys->verKey($classKey),
                $modelVersion
            );
        }

        return $loaded->all();
    }

    private static function hydrateClosure(): \Closure
    {
        return self::$hydrateClosure ??= \Closure::bind(
            static function (Model $instance, array $attrs, bool $fire): void {
                $instance->setRawAttributes($attrs, true);
                $instance->exists = true;
                if ($fire) {
                    $instance->fireModelEvent('retrieved', false);
                }
            },
            null,
            Model::class
        );
    }

    private function prepareMissedQuery(string $modelClass, ?CacheableBuilder $missedQuery, bool $preserveQueryShape): EloquentBuilder
    {
        if ($missedQuery === null || !$preserveQueryShape) {
            $builder = $modelClass::query();
            if ($builder instanceof CacheableBuilder) {
                $missedQuery?->applyRemovedScopesTo($builder);

                return $builder->withoutCache();
            }

            return $builder;
        }

        $base = $missedQuery->getQuery()
            ->cloneWithout(['columns', 'orders', 'limit', 'offset'])
            ->cloneWithoutBindings(['select', 'order']);

        $builder = (new CacheableBuilder($base))
            ->setModel($missedQuery->getModel())
            ->withoutCache();

        $missedQuery->applyRemovedScopesTo($builder);

        return $builder;
    }
}
