<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Collection;
use NormCache\CacheableBuilder;
use NormCache\Support\AttributeProjector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;

final class ModelHydrator
{
    private static ?\Closure $hydrateClosure = null;

    private static ?\Closure $transformScalarClosure = null;

    private static ?\Closure $transformScalarsClosure = null;

    private const STATELESS_CASTS = [
        'array' => true,
        'bool' => true,
        'boolean' => true,
        'collection' => true,
        'custom_datetime' => true,
        'date' => true,
        'datetime' => true,
        'decimal' => true,
        'double' => true,
        'float' => true,
        'immutable_custom_datetime' => true,
        'immutable_date' => true,
        'immutable_datetime' => true,
        'int' => true,
        'integer' => true,
        'json' => true,
        'json:unicode' => true,
        'object' => true,
        'real' => true,
        'string' => true,
        'timestamp' => true,
    ];

    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $ttl,
        private readonly bool $fireRetrieved,
        private readonly int $buildingLockTtl = 5,
        private readonly int $stampedeWaitMs = 200,
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

        $reporting = CacheReporter::active();
        $debugbarStart = $reporting ? CacheReporter::beginMeasure() : null;

        $containedInQueryHit = $raw !== null;

        // arrays may come with sparse numeric keys, for example after array_unique().
        $ids = array_values($ids);
        $classKey = $this->keys->classKey($modelClass);
        $projection = $columns !== null ? AttributeProjector::normalizeProjection($columns) : null;

        $raw ??= $this->store->getMany($this->modelKeysFor($classKey, $ids));

        ['hits' => $hits, 'missed' => $missed] = $this->hydrateModels($ids, $modelClass, $raw, $projection, $prototype);
        $lockKey = $wakeKey = $token = null;
        $version = 0;

        if ($hits !== [] && $reporting) {
            CacheReporter::modelHitActive($modelClass, array_keys($hits), $debugbarStart, [
                'suppress_collector' => $containedInQueryHit,
                'ids' => $ids,
            ]);
        }

        if ($missed === []) {
            return array_values($hits);
        }

        if ($reporting) {
            CacheReporter::modelMissActive($modelClass, $missed, $debugbarStart, [
                'hits' => array_keys($hits),
                'partial' => $hits !== [],
            ]);
        }

        [$lockKey, $wakeKey, $token] = $this->buildLockTriple($classKey, $missed);

        [$status, $missed, $version] = $this->fetchMissedStatus($missed, $modelClass, $classKey, $projection, $prototype, $lockKey, $token, $hits);

        if ($status === 'building' && $missed !== []) {
            $this->store->brpop($wakeKey, $this->stampedeWaitMs / 1000.0);

            [$status, $missed, $version] = $this->fetchMissedStatus($missed, $modelClass, $classKey, $projection, $prototype, $lockKey, $token, $hits);
        }

        if ($status === 'miss') {
            try {
                $this->fetchAndMerge($missed, $modelClass, $classKey, $projection, $missedQuery, $preserveQueryShape, $hits, $version, $lockKey, $wakeKey, $token);
            } catch (\Throwable $e) {
                $this->store->releaseBuilding($lockKey, $wakeKey, $token);

                throw $e;
            }
        } elseif ($missed !== []) {
            $this->fetchAndMerge($missed, $modelClass, $classKey, $projection, $missedQuery, $preserveQueryShape, $hits, $version);
        }

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($hits[$id])) {
                $ordered[] = $hits[$id];
            }
        }

        return $ordered;
    }

    /** Builds the building-lock key/wake-key/token triple for a set of model ids. */
    private function buildLockTriple(string $classKey, array $ids): array
    {
        $sorted = $ids;
        sort($sorted);
        $lockSuffix = $this->keys->resultBuildIdentityHash('model', null, implode(',', $sorted));
        $lockKey = $this->keys->resultBuildingKey($classKey, 'model', $lockSuffix);
        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);
        $token = $this->versions->buildLockToken();

        return [$lockKey, $wakeKey, $token];
    }

    private function modelKeysFor(string $classKey, array $ids): array
    {
        $prefix = $this->keys->modelPrefix($classKey);
        $keys = [];
        foreach ($ids as $id) {
            $keys[] = $prefix . $id;
        }

        return $keys;
    }

    /**
     * Re-checks ids known to be missing via a native MGET (cheaper than Lua doing its own
     * MGET for the same presence check) — this also catches another worker having filled
     * them between the caller's MGET and this one. Only claims the build lock, via a
     * status-only Lua script, if ids are still missing after that.
     */
    private function fetchMissedStatus(
        array $idsToFetch,
        string $modelClass,
        string $classKey,
        ?array $projection,
        ?Model $prototype,
        string $lockKey,
        string $token,
        array &$hits,
    ): array {
        $fetchKeys = $this->modelKeysFor($classKey, $idsToFetch);
        $raw = $this->store->getMany($fetchKeys);

        ['hits' => $newHits, 'missed' => $stillMissed] = $this->hydrateModels($idsToFetch, $modelClass, $raw, $projection, $prototype);

        foreach ($newHits as $id => $hit) {
            $hits[$id] = $hit;
        }

        if ($stillMissed === []) {
            return ['hit', [], 0];
        }

        $result = $this->store->script(
            RedisScripts::get('fetch_model_build_status'),
            [$lockKey, $this->keys->verKey($classKey)],
            [$token, (string) $this->buildingLockTtl]
        );

        $status = $result[0];
        $version = $this->versions->normalizeVersion($result[2] ?? null);

        return [$status, $stillMissed, $version];
    }

    /** Fetches still-missing ids from the database, caches them, and merges into $hits by reference. */
    private function fetchAndMerge(
        array $missed,
        string $modelClass,
        string $classKey,
        ?array $projection,
        ?EloquentBuilder $missedQuery,
        bool $preserveQueryShape,
        array &$hits,
        int $modelVersion,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
    ): void {
        if ($missed === []) {
            return;
        }

        $fetched = $this->fetchFromDatabaseAndCache(
            $missed, $modelClass, $classKey, $projection, $missedQuery, $preserveQueryShape,
            $modelVersion, $buildingKey, $wakeKey, $token
        );

        foreach ($fetched as $id => $model) {
            $hits[$id] = $model;
        }
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

        if ($cached && $models !== [] && CacheReporter::active()) {
            $keys = [];
            foreach ($models as $m) {
                $key = $m->getKey();
                if ($key !== null) {
                    $keys[] = $key;
                }
            }
            CacheReporter::modelHit($modelClass, $keys, null);
        }

        return $models;
    }

    public static function transformScalar(mixed $value, Model $model, string $column): mixed
    {
        $isCast = self::resolveStatelessScalarMode($model, $column);

        if ($isCast === null) {
            return $model->newFromBuilder([$column => $value])->{$column};
        }

        return self::transformScalarClosure()($model, $column, $value, $isCast);
    }

    public static function transformScalars(Collection $results, Model $model, string $column): Collection
    {
        $isCast = self::resolveStatelessScalarMode($model, $column);
        $values = $results->all();

        if ($isCast === null) {
            $template = $model->newInstance([], true);
            $hydrate = self::hydrateClosure();

            foreach ($values as $key => $value) {
                $instance = clone $template;
                $hydrate($instance, [$column => $value], true);
                $values[$key] = $instance->{$column};
            }

            return new Collection($values);
        }

        return new Collection(
            self::transformScalarsClosure()($model, $column, $values, $isCast),
        );
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
        int $modelVersion,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
    ): array {
        $query = $this->prepareMissedQuery(
            $modelClass,
            $missedQuery instanceof CacheableBuilder ? $missedQuery : null,
            $preserveQueryShape
        );

        if ($this->overridesNewFromBuilder($query->getModel())) {
            return $this->fetchAndCacheUsingEloquent(
                $missed, $modelClass, $classKey, $projection, $query, $modelVersion, $buildingKey, $wakeKey, $token
            );
        }

        return $this->fetchAndCacheUsingClosure(
            $missed, $modelClass, $classKey, $projection, $query, $query->getModel(), $modelVersion, $buildingKey, $wakeKey, $token
        );
    }

    private function fetchAndCacheUsingEloquent(
        array $missed,
        string $modelClass,
        string $classKey,
        ?array $projection,
        EloquentBuilder $query,
        int $modelVersion,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
    ): array {
        $prototype = CacheKeyBuilder::prototype($modelClass);
        $pk = $prototype->getKeyName();
        $qualifiedPk = $prototype->getQualifiedKeyName();
        $loaded = $query->whereIn($qualifiedPk, $missed)
            ->get(['*'])
            ->keyBy($pk);

        $attrsByKey = [];
        $deletedAtCol = CacheKeyBuilder::deletedAtColumn($modelClass);
        $hydrate = $projection !== null ? self::hydrateClosure() : null;

        foreach ($loaded as $id => $model) {
            $attrs = $model->getRawOriginal();

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if (!$isTrashed) {
                $attrsByKey[$this->keys->modelPrefix($classKey) . $id] = $attrs;
            }

            if ($hydrate !== null) {
                $hydrate($model, AttributeProjector::projectAttributes($attrs, $projection), false);
            }
        }

        if ($attrsByKey !== [] || $buildingKey !== null) {
            $this->store->setManyTrackedIfVersion(
                $attrsByKey,
                $this->ttl,
                $this->keys->membersKey($classKey),
                $this->keys->verKey($classKey),
                $modelVersion,
                $buildingKey,
                $wakeKey,
                $token
            );
        }

        return $loaded->all();
    }

    private function fetchAndCacheUsingClosure(
        array $missed,
        string $modelClass,
        string $classKey,
        ?array $projection,
        EloquentBuilder $query,
        Model $prototype,
        int $modelVersion,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
    ): array {
        $pk = $prototype->getKeyName();
        $qualifiedPk = $prototype->getQualifiedKeyName();
        $deletedAtCol = CacheKeyBuilder::deletedAtColumn($modelClass);
        $hydrate = self::hydrateClosure();
        $connectionName = $query->getModel()->getConnectionName();

        $rows = $query->whereIn($qualifiedPk, $missed)
            ->toBase()
            ->get(['*']);

        $models = [];
        $attrsByKey = [];

        foreach ($rows as $row) {
            $attrs = (array) $row;

            if (!array_key_exists($pk, $attrs)) {
                continue;
            }

            $id = $attrs[$pk];

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if (!$isTrashed) {
                $attrsByKey[$this->keys->modelPrefix($classKey) . $id] = $attrs;
            }

            $returnAttrs = $projection !== null
                ? AttributeProjector::projectAttributes($attrs, $projection)
                : $attrs;

            $instance = clone $prototype;
            $instance->setConnection($connectionName);
            $hydrate($instance, $returnAttrs, true);

            $models[$id] = $instance;
        }

        if ($attrsByKey !== [] || $buildingKey !== null) {
            $this->store->setManyTrackedIfVersion(
                $attrsByKey,
                $this->ttl,
                $this->keys->membersKey($classKey),
                $this->keys->verKey($classKey),
                $modelVersion,
                $buildingKey,
                $wakeKey,
                $token
            );
        }

        return $models;
    }

    private function overridesNewFromBuilder(Model $model): bool
    {
        $method = new \ReflectionMethod($model, 'newFromBuilder');

        return $method->getDeclaringClass()->getName() !== Model::class;
    }

    public static function hydrateClosure(): \Closure
    {
        return self::$hydrateClosure ??= \Closure::bind(
            static function (Model $instance, array $attrs, bool $fire): void {
                $instance->attributes = $attrs;
                $instance->original = $attrs;
                $instance->classCastCache = [];
                $instance->attributeCastCache = [];
                $instance->exists = true;
                if ($fire) {
                    $instance->fireModelEvent('retrieved', false);
                }
            },
            null,
            Model::class
        );
    }

    private static function resolveStatelessScalarMode(Model $model, string $column): ?bool
    {
        if ($model->hasAnyGetMutator($column)) {
            return null;
        }

        $cast = $model->getCasts()[$column] ?? null;

        if ($cast === null) {
            if (!in_array($column, $model->getDates(), true)) {
                return null;
            }

            $isCast = false;
        } elseif (!is_string($cast)) {
            return null;
        } else {
            $cast = strtolower(explode(':', $cast, 2)[0]);

            if (!isset(self::STATELESS_CASTS[$cast])) {
                return null;
            }

            $isCast = true;
        }

        $dispatcher = $model::getEventDispatcher();

        if ($dispatcher !== null && $dispatcher->hasListeners('eloquent.retrieved: ' . $model::class)) {
            return null;
        }

        return $isCast;
    }

    private static function transformScalarClosure(): \Closure
    {
        return self::$transformScalarClosure ??= \Closure::bind(
            static function (Model $model, string $column, mixed $value, bool $isCast): mixed {
                if ($isCast) {
                    return $model->castAttribute($column, $value);
                }

                return $value === null ? null : $model->asDateTime($value);
            },
            null,
            Model::class
        );
    }

    private static function transformScalarsClosure(): \Closure
    {
        return self::$transformScalarsClosure ??= \Closure::bind(
            static function (Model $model, string $column, array $values, bool $isCast): array {
                if ($isCast) {
                    foreach ($values as $key => $value) {
                        $values[$key] = $model->castAttribute($column, $value);
                    }

                    return $values;
                }

                foreach ($values as $key => $value) {
                    $values[$key] = $value === null ? null : $model->asDateTime($value);
                }

                return $values;
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
