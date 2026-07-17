<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Enums\LuaStatus;
use NormCache\Support\AttributeProjector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RawAttributes;
use NormCache\Support\RedisStore;
use NormCache\Values\ModelFetchContext;

final class ModelHydrator
{
    private static array $overridesNewFromBuilder = [];

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
        $connectionModel = $prototype ?? $missedQuery?->getModel() ?? CacheKeyBuilder::prototype($modelClass);
        $connection = $connectionModel->getConnection()->getName()
            ?? $connectionModel->getConnectionName()
            ?? '';
        $classKey = $this->keys->classKey($modelClass, $connection);
        $projection = $columns !== null ? AttributeProjector::normalizeProjection($columns) : null;

        // Pre-supplied $raw skips the version GET; resolve lazily only if there are misses.
        if ($raw === null) {
            $modelVersion = $this->versions->currentVersion($modelClass, $this->keys->activeSpace(), $connection);
            $raw = $this->store->getMany($this->modelKeysFor($classKey, $modelVersion, $ids));
        } else {
            $modelVersion = 0; // deferred
        }

        ['hits' => $hits, 'missed' => $missed, 'ordered' => $orderedHits] = $this->hydrateModelPayload(
            $ids,
            $modelClass,
            $raw,
            $projection,
            $prototype,
        );

        if ($missed === []) {
            if ($reporting) {
                CacheReporter::modelHitActive($modelClass, $ids, $debugbarStart, [
                    'ids' => $ids,
                ]);
            }

            return $orderedHits;
        }

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

        if ($versionDeferred) {
            $context->modelVersion = $this->versions->currentVersion($modelClass, $this->keys->activeSpace(), $connection);
        }

        if ($reporting) {
            CacheReporter::modelMissActive($modelClass, $missed, $debugbarStart, [
                'hits' => array_keys($context->hits),
                'partial' => $context->hits !== [],
            ]);
        }

        [$context->lockKey, $context->wakeKey, $context->token] = $this->buildLockTriple($classKey, $context->modelVersion, $missed);

        [$status, $missed] = $this->fetchMissedStatus($missed, $context);

        if ($status === LuaStatus::Building && $missed !== []) {
            $this->store->brpop($context->wakeKey, $this->stampedeWaitMs / 1000.0);

            [$status, $missed] = $this->fetchMissedStatus($missed, $context);
        }

        if ($status === LuaStatus::Miss) {
            try {
                $this->fetchAndMerge($missed, $context, true);
            } catch (\Throwable $e) {
                $this->store->releaseBuilding($context->lockKey, $context->wakeKey, $context->token);

                throw $e;
            }
        } elseif ($status === LuaStatus::Hit && $missed !== []) {
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
        } elseif ($status === LuaStatus::Building && $missed !== []) {
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
    /** @return array{0: LuaStatus, 1: list<int|string>} */
    private function fetchMissedStatus(array $idsToFetch, ModelFetchContext $context): array
    {
        $fetchKeys = $this->modelKeysFor($context->classKey, $context->modelVersion, $idsToFetch);

        $result = $this->store->fetchBatchBuildStatus($fetchKeys, $context->lockKey, $context->wakeKey, $context->token, $this->buildingLockTtl);

        $raw = $this->store->unserializeMany($result[3] ?? []);

        ['hits' => $newHits, 'missed' => $stillMissed] = $this->hydrateModelPayload(
            $idsToFetch,
            $context->modelClass,
            $raw,
            $context->projection,
            $context->prototype,
        );

        foreach ($newHits as $id => $hit) {
            $context->hits[$id] = $hit;
        }

        if ($stillMissed === []) {
            return [LuaStatus::Hit, []];
        }

        return [LuaStatus::fromLua($result[0] ?? null), $stillMissed];
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
        $hydrate = RawAttributes::hydrateClosure();

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

    /** @return array{hits: array<int|string, Model>, missed: list<int|string>, ordered: list<Model>} */
    private function hydrateModelPayload(array $ids, string $modelClass, array $raw, ?array $projection, ?Model $prototype = null): array
    {
        $prototype = $prototype ?? CacheKeyBuilder::prototype($modelClass);
        $fire = $this->fireRetrieved;
        $hydrate = RawAttributes::hydrateClosure();
        $hits = [];
        $missed = [];
        $ordered = [];
        $seen = [];

        foreach ($ids as $i => $id) {
            if (isset($seen[$id])) {
                continue;
            }
            $seen[$id] = true;

            $attrs = $raw[$i] ?? null;

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
            $ordered[] = $instance;
        }

        return ['hits' => $hits, 'missed' => $missed, 'ordered' => $ordered];
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
        $loaded = $query->whereIn($prototype->getQualifiedKeyName(), $missed)
            ->get([$prototype->getTable() . '.*']);
        $hydrate = $context->projection !== null ? RawAttributes::hydrateClosure() : null;

        return $this->cacheAndCollect($loaded, $context, $writeCache, function ($model) use ($pk, $hydrate, $context) {
            $attrs = $model->getRawOriginal();

            if ($hydrate !== null) {
                $hydrate($model, AttributeProjector::projectAttributes($attrs, $context->projection), false);
            }

            return array_key_exists($pk, $attrs) ? [$attrs[$pk], $attrs, $model] : null;
        });
    }

    private function fetchAndCacheUsingClosure(
        array $missed,
        EloquentBuilder $query,
        ModelFetchContext $context,
        bool $writeCache,
    ): array {
        $prototype = $query->getModel();
        $pk = $prototype->getKeyName();
        $hydrate = RawAttributes::hydrateClosure();
        $connectionName = $prototype->getConnectionName();

        $rows = $query->whereIn($prototype->getQualifiedKeyName(), $missed)
            ->toBase()
            ->get([$prototype->getTable() . '.*']);

        return $this->cacheAndCollect($rows, $context, $writeCache, function ($row) use ($pk, $prototype, $hydrate, $connectionName, $context) {
            $attrs = (array) $row;

            if (!array_key_exists($pk, $attrs)) {
                return null;
            }

            $returnAttrs = $context->projection !== null
                ? AttributeProjector::projectAttributes($attrs, $context->projection)
                : $attrs;

            $instance = clone $prototype;
            $instance->setConnection($connectionName);
            $hydrate($instance, $returnAttrs, true);

            return [$attrs[$pk], $attrs, $instance];
        });
    }

    /**
     * Shared miss-path bookkeeping: cache-write non-trashed rows, key models by id.
     *
     * @param  callable(mixed): (array{mixed, array<string, mixed>, Model}|null)  $each  row → [id, raw attrs, model]; null skips the row
     */
    private function cacheAndCollect(iterable $rows, ModelFetchContext $context, bool $writeCache, callable $each): array
    {
        $deletedAtCol = CacheKeyBuilder::deletedAtColumn($context->modelClass);
        $prefix = $this->keys->modelPrefix($context->classKey, $context->modelVersion);
        $attrsByKey = [];
        $models = [];

        foreach ($rows as $row) {
            $result = $each($row);

            if ($result === null) {
                continue;
            }

            [$id, $attrs, $model] = $result;

            if ($writeCache && !($deletedAtCol && isset($attrs[$deletedAtCol]))) {
                $attrsByKey[$prefix . $id] = $attrs;
            }

            $models[$id] = $model;
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

    private function prepareMissedQuery(string $modelClass, ?CacheableBuilder $missedQuery, bool $preserveQueryShape): EloquentBuilder
    {
        if ($missedQuery === null || !$preserveQueryShape || !$this->canPreserveQueryShape($missedQuery->getQuery())) {
            $builder = $missedQuery !== null
                ? $missedQuery->getModel()->newQuery()
                : $modelClass::query();

            if ($builder instanceof CacheableBuilder) {
                if ($missedQuery !== null) {
                    $builder->withoutGlobalScopes($missedQuery->removedScopes());
                }

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

        $builder->withoutGlobalScopes($missedQuery->removedScopes());

        return $builder;
    }

    private function canPreserveQueryShape(QueryBuilder $base): bool
    {
        return $base->unions === null && is_string($base->from);
    }
}
