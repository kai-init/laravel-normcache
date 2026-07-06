<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Collection;
use NormCache\CacheableBuilder;
use NormCache\Support\AttributeProjector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisStore;
use NormCache\Values\ModelFetchContext;

final class ModelHydrator
{
    private static ?\Closure $hydrateClosure = null;

    private static ?\Closure $setAttributeDirectClosure = null;

    private static array $overridesNewFromBuilder = [];

    private static ?\Closure $getAttributeDirectClosure = null;

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

        // Pre-supplied $raw defers the version GET; remember it since $raw is reassigned below.
        $versionDeferred = $raw !== null;

        // arrays may come with sparse numeric keys, for example after array_unique().
        $ids = array_values($ids);
        $classKey = $this->keys->classKey($modelClass);
        $projection = $columns !== null ? AttributeProjector::normalizeProjection($columns) : null;

        // Pre-supplied $raw skips the version GET; resolve lazily only if there are misses.
        if ($raw === null) {
            $modelVersion = $this->versions->currentVersion($modelClass, $this->keys->activeSpace());
            $raw = $this->store->getMany($this->modelKeysFor($classKey, $modelVersion, $ids));
        } else {
            $modelVersion = 0; // deferred
        }

        ['hits' => $hits, 'missed' => $missed] = $this->hydrateModels($ids, $modelClass, $raw, $projection, $prototype);

        $context = new ModelFetchContext(
            modelClass: $modelClass,
            classKey: $classKey,
            projection: $projection,
            prototype: $prototype,
            missedQuery: $missedQuery,
            preserveQueryShape: $preserveQueryShape,
            modelVersion: $modelVersion,
        );
        $context->hits = $hits;

        if ($context->hits !== [] && $reporting) {
            CacheReporter::modelHitActive($modelClass, array_keys($context->hits), $debugbarStart, [
                'ids' => $ids,
            ]);
        }

        if ($missed === []) {
            return array_values($context->hits);
        }

        if ($versionDeferred) {
            $context->modelVersion = $this->versions->currentVersion($modelClass, $this->keys->activeSpace());
        }

        if ($reporting) {
            CacheReporter::modelMissActive($modelClass, $missed, $debugbarStart, [
                'hits' => array_keys($context->hits),
                'partial' => $context->hits !== [],
            ]);
        }

        [$context->lockKey, $context->wakeKey, $context->token] = $this->buildLockTriple($classKey, $context->modelVersion, $missed);

        [$status, $missed] = $this->fetchMissedStatus($missed, $context);

        if ($status === 'building' && $missed !== []) {
            $this->store->brpop($context->wakeKey, $this->stampedeWaitMs / 1000.0);

            [$status, $missed] = $this->fetchMissedStatus($missed, $context);
        }

        if ($status === 'miss') {
            try {
                $this->fetchAndMerge($missed, $context, true);
            } catch (\Throwable $e) {
                $this->store->releaseBuilding($context->lockKey, $context->wakeKey, $context->token);

                throw $e;
            }
        } elseif ($status === 'hit' && $missed !== []) {
            // Corrupt payload: Lua reported hit but deserialization failed. Only the lock owner overwrites.
            if ($this->store->setNxEx($context->lockKey, $context->token, $this->buildingLockTtl)) {
                try {
                    $this->fetchAndMerge($missed, $context, true);
                } catch (\Throwable $e) {
                    $this->store->releaseBuilding($context->lockKey, $context->wakeKey, $context->token);
                    throw $e;
                }
            } else {
                $this->fetchAndMerge($missed, $context, false);
            }
        } elseif ($status === 'building' && $missed !== []) {
            $this->fetchAndMerge($missed, $context, false);
        }

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($context->hits[$id])) {
                $ordered[] = $context->hits[$id];
            }
        }

        return $ordered;
    }

    // Builds the building-lock key/wake-key/token triple for a set of model ids.
    private function buildLockTriple(string $classKey, int $modelVersion, array $ids): array
    {
        $sorted = $ids;
        sort($sorted);
        $segment = 'model:v' . $modelVersion;
        $lockSuffix = $this->keys->resultBuildIdentityHash($segment, null, implode(',', $sorted));
        $lockKey = $this->keys->resultBuildingKey($classKey, $segment, $lockSuffix);
        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);
        $token = $this->versions->buildLockToken();

        return [$lockKey, $wakeKey, $token];
    }

    private function modelKeysFor(string $classKey, int $modelVersion, array $ids): array
    {
        $prefix = $this->keys->modelPrefix($classKey, $modelVersion);
        $keys = [];
        foreach ($ids as $id) {
            $keys[] = $prefix . $id;
        }

        return $keys;
    }

    // Re-checks still-missing ids and atomically claims the build lock if anything's still missing.
    private function fetchMissedStatus(array $idsToFetch, ModelFetchContext $context): array
    {
        $fetchKeys = $this->modelKeysFor($context->classKey, $context->modelVersion, $idsToFetch);

        $result = $this->store->fetchBatchBuildStatus($fetchKeys, $context->lockKey, $context->wakeKey, $context->token, $this->buildingLockTtl);

        $raw = $this->store->unserializeMany($result[3] ?? []);

        ['hits' => $newHits, 'missed' => $stillMissed] = $this->hydrateModels($idsToFetch, $context->modelClass, $raw, $context->projection, $context->prototype);

        foreach ($newHits as $id => $hit) {
            $context->hits[$id] = $hit;
        }

        if ($stillMissed === []) {
            return ['hit', []];
        }

        return [$result[0], $stillMissed];
    }

    private function fetchAndMerge(array $missed, ModelFetchContext $context, bool $writeCache): void
    {
        if ($missed === []) {
            return;
        }

        $fetched = $this->fetchFromDatabaseAndCache($missed, $context, $writeCache);

        foreach ($fetched as $id => $model) {
            $context->hits[$id] = $model;
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

    private function fetchFromDatabaseAndCache(array $missed, ModelFetchContext $context, bool $writeCache): array
    {
        $query = $this->prepareMissedQuery(
            $context->modelClass,
            $context->missedQuery instanceof CacheableBuilder ? $context->missedQuery : null,
            $context->preserveQueryShape
        );

        if ($this->overridesNewFromBuilder($query->getModel())) {
            return $this->fetchAndCacheUsingEloquent($missed, $query, $context, $writeCache);
        }

        return $this->fetchAndCacheUsingClosure($missed, $query, $context, $writeCache);
    }

    private function fetchAndCacheUsingEloquent(
        array $missed,
        EloquentBuilder $query,
        ModelFetchContext $context,
        bool $writeCache,
    ): array {
        $prototype = CacheKeyBuilder::prototype($context->modelClass);
        $pk = $prototype->getKeyName();
        $qualifiedPk = $prototype->getQualifiedKeyName();
        $loaded = $query->whereIn($qualifiedPk, $missed)
            ->get([$prototype->getTable() . '.*'])
            ->keyBy($pk);

        $attrsByKey = [];
        $deletedAtCol = CacheKeyBuilder::deletedAtColumn($context->modelClass);
        $hydrate = $context->projection !== null ? self::hydrateClosure() : null;

        foreach ($loaded as $id => $model) {
            $attrs = $model->getRawOriginal();

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if ($writeCache && !$isTrashed) {
                $attrsByKey[$this->keys->modelPrefix($context->classKey, $context->modelVersion) . $id] = $attrs;
            }

            if ($hydrate !== null) {
                $hydrate($model, AttributeProjector::projectAttributes($attrs, $context->projection), false);
            }
        }

        $this->storeModelAttrs($attrsByKey, $context, $writeCache);

        return $loaded->all();
    }

    private function fetchAndCacheUsingClosure(
        array $missed,
        EloquentBuilder $query,
        ModelFetchContext $context,
        bool $writeCache,
    ): array {
        $prototype = $query->getModel();
        $pk = $prototype->getKeyName();
        $qualifiedPk = $prototype->getQualifiedKeyName();
        $deletedAtCol = CacheKeyBuilder::deletedAtColumn($context->modelClass);
        $hydrate = self::hydrateClosure();
        $connectionName = $prototype->getConnectionName();

        $rows = $query->whereIn($qualifiedPk, $missed)
            ->toBase()
            ->get([$prototype->getTable() . '.*']);

        $models = [];
        $attrsByKey = [];

        foreach ($rows as $row) {
            $attrs = (array) $row;

            if (!array_key_exists($pk, $attrs)) {
                continue;
            }

            $id = $attrs[$pk];

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if ($writeCache && !$isTrashed) {
                $attrsByKey[$this->keys->modelPrefix($context->classKey, $context->modelVersion) . $id] = $attrs;
            }

            $returnAttrs = $context->projection !== null
                ? AttributeProjector::projectAttributes($attrs, $context->projection)
                : $attrs;

            $instance = clone $prototype;
            $instance->setConnection($connectionName);
            $hydrate($instance, $returnAttrs, true);

            $models[$id] = $instance;
        }

        $this->storeModelAttrs($attrsByKey, $context, $writeCache);

        return $models;
    }

    // Passes the lock triple only when this call owns the build lock ($writeCache), so the Lua release is owner-gated.
    private function storeModelAttrs(array $attrsByKey, ModelFetchContext $context, bool $writeCache): void
    {
        $this->store->setManyIfVersion(
            $attrsByKey,
            $this->ttl,
            $this->keys->verKey($context->classKey),
            $context->modelVersion,
            $writeCache ? $context->lockKey : null,
            $writeCache ? $context->wakeKey : null,
            $writeCache ? $context->token : null,
        );
    }

    public static function reset(): void
    {
        self::$overridesNewFromBuilder = [];
    }

    private function overridesNewFromBuilder(Model $model): bool
    {
        $class = $model::class;

        return self::$overridesNewFromBuilder[$class] ??=
            (new \ReflectionMethod($model, 'newFromBuilder'))->getDeclaringClass()->getName() !== Model::class;
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

    public static function setAttributeDirectClosure(): \Closure
    {
        return self::$setAttributeDirectClosure ??= \Closure::bind(
            static function (Model $instance, string $key, mixed $value): void {
                $instance->attributes[$key] = $value;
            },
            null,
            Model::class
        );
    }

    public static function getAttributeDirectClosure(): \Closure
    {
        return self::$getAttributeDirectClosure ??= \Closure::bind(
            static function (Model $instance, string $key): mixed {
                return $instance->attributes[$key] ?? null;
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
        if ($missedQuery === null || !$preserveQueryShape || !$this->canPreserveQueryShape($missedQuery->getQuery())) {
            $builder = $modelClass::query();
            if ($builder instanceof CacheableBuilder) {
                $missedQuery?->applyRemovedScopesTo($builder);

                return $builder->withoutCache();
            }

            return $builder;
        }

        // Groups/havings are stripped: we already know the primary keys and want each one's raw row, not an aggregate.
        $base = $missedQuery->getQuery()
            ->cloneWithout(['columns', 'orders', 'limit', 'offset', 'groups', 'havings'])
            ->cloneWithoutBindings(['select', 'order', 'groupBy', 'having']);

        $builder = (new CacheableBuilder($base))
            ->setModel($missedQuery->getModel())
            ->withoutCache();

        $missedQuery->applyRemovedScopesTo($builder);

        return $builder;
    }

    private function canPreserveQueryShape(QueryBuilder $base): bool
    {
        return $base->unions === null && is_string($base->from);
    }
}
