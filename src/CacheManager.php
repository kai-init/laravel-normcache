<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\LuaScripts;
use NormCache\Support\QueryInspector;
use NormCache\Support\RedisStore;
use NormCache\Traits\HandlesInvalidation;

class CacheManager
{
    use HandlesInvalidation;

    private static array $hydratorClosures = [];

    private static array $deletedAtColumns = [];

    private RedisStore $store;

    private CacheKeyBuilder $keys;

    private bool $cluster;

    private bool $slotting;

    public function __construct(
        string $redisConnection,
        private int $ttl,
        private int $queryTtl,
        string $keyPrefix,
        private int $cooldown,
        bool $cluster = false,
        private bool $enabled = true,
        private bool $dispatchEvents = true,
        private bool $fallbackEnabled = false,
        private bool $fireRetrieved = false,
        private int $buildingLockTtl = 5,
        private int $stampedeWaitMs = 200,
        private int $staleVersionDepth = 3,
        bool $slotting = false,
    ) {
        $this->cluster = $cluster;
        $this->slotting = $cluster && $slotting;
        $this->store = new RedisStore($redisConnection, $keyPrefix, $this->slotting, $slotting ? '' : '{nc}:');
        $this->keys = new CacheKeyBuilder;
    }

    // -------------------------------------------------------------------------
    // Lifecycle / configuration
    // -------------------------------------------------------------------------

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function isEventsEnabled(): bool
    {
        return $this->dispatchEvents;
    }

    public function isCluster(): bool
    {
        return $this->cluster;
    }

    public function enable(): void
    {
        $this->enabled = true;
    }

    public function disable(): void
    {
        $this->enabled = false;
    }

    public function getStore(): RedisStore
    {
        return $this->store;
    }

    // -------------------------------------------------------------------------
    // High-level cache reads
    // -------------------------------------------------------------------------

    public function getModelsFromQuery(string $modelClass, string $hash, ?string $tag = null): array
    {
        $classKey = $this->keys->classKey($modelClass);

        $result = $this->luaFetchVersionedQuery($classKey, $hash, (int) floor(microtime(true) * 1000), $tag);

        $luaStatus = $result[0] ?? null;
        $version = $this->normalizeVersion($result[1]);
        $queryKey = $this->keys->queryKey($classKey, $tag, $version, $hash);

        $deserialize = fn($r) => $this->store->unserializeMany($r);

        return match ($luaStatus) {
            'hit' => $this->queryResult('hit', $queryKey, $result[2], $deserialize($result[3]), null),
            'stale' => $this->queryResult('stale', null, $result[2], $deserialize($result[3]), null),
            'empty' => $this->queryResult('empty', $queryKey, [], [], null),
            'miss' => $this->queryResult('miss', $queryKey, null, null, $this->keys->buildingPrefix($classKey) . $hash, [$this->keys->verKey($classKey)], [(string) $version]),
            'building' => $this->queryResult('building', null, null, null, null),
            default => $this->queryResult('miss', $queryKey, null, null, null),
        };
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash, array $depClasses = [], ?string $tag = null): array
    {
        $classKey = $this->keys->classKey($modelClass);
        $versionKeys = $this->keys->depVersionKeys($classKey, $depClasses);
        $scheduledKeys = $this->keys->depScheduledKeys($classKey, $depClasses);
        $keyPrefix = $this->keys->namespacedPrefix($namespace, $classKey, $tag);

        if ($this->slotting) {
            [$seg, $data] = $this->clusterFetchVersionedCache($versionKeys, $scheduledKeys, $keyPrefix, $hash);

            return ['key' => $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash), 'data' => $data];
        }

        [$seg, $blob] = $this->luaFetchVersionedCache($versionKeys, $scheduledKeys, $keyPrefix, $hash);

        return [
            'key' => $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash),
            'data' => $blob !== false ? $this->store->unserialize($blob) : null,
        ];
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->keys->classKey($relatedClass);
        $throughKey = $this->keys->classKey($throughClass);
        $versionKeys = [$this->keys->verKey($relatedKey), $this->keys->verKey($throughKey)];
        $scheduledKeys = [$this->keys->scheduledKey($relatedKey), $this->keys->scheduledKey($throughKey)];
        $keyPrefix = $this->keys->throughPrefix($relatedKey, $throughKey);

        if ($this->slotting) {
            [$seg, $data] = $this->clusterFetchVersionedCache($versionKeys, $scheduledKeys, $keyPrefix, $hash);

            return ['key' => $this->keys->throughKey($relatedKey, $throughKey, $seg, $hash), 'data' => $data];
        }

        [$seg, $blob] = $this->luaFetchVersionedCache($versionKeys, $scheduledKeys, $keyPrefix, $hash);

        return ['key' => $this->keys->throughKey($relatedKey, $throughKey, $seg, $hash), 'data' => $blob !== false ? $this->store->unserialize($blob) : null];
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc'): array
    {
        $parentKey = $this->keys->classKey($parentClass);
        $relatedKey = $this->keys->classKey($relatedClass);

        if ($this->slotting) {
            $seg = $this->resolveDepVersionsSeg(
                [$this->keys->verKey($parentKey), $this->keys->verKey($relatedKey)],
                [$this->keys->scheduledKey($parentKey), $this->keys->scheduledKey($relatedKey)],
                (int) floor(microtime(true) * 1000)
            );
            $pivotKeys = array_map(fn($id) => $this->keys->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id), $parentIds);

            return [
                'seg' => $seg,
                'data' => array_combine($parentIds, $this->store->getMany($pivotKeys)),
            ];
        }

        [$seg, $blobs] = $this->luaFetchVersionedPivotCache($parentKey, $relatedKey, $relation, $constraintHash, $parentIds);

        return [
            'seg' => $seg,
            'data' => array_combine($parentIds, $this->store->unserializeMany($blobs)),
        ];
    }

    public function getRawCache(string $modelClass, array $depClasses, string $hash, ?string $tag = null): array
    {
        $classKey = $this->keys->classKey($modelClass);
        $lockSuffix = $this->keys->rawBuildLockSuffix($tag, $hash);
        $versionKeys = $this->keys->depVersionKeys($classKey, $depClasses);
        $scheduledKeys = $this->keys->depScheduledKeys($classKey, $depClasses);

        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);

        if ($this->slotting) {
            $seg = $this->resolveDepVersionsSeg($versionKeys, $scheduledKeys, (int) floor(microtime(true) * 1000));
            $rawKey = $this->keys->rawKey($classKey, $tag, $seg, $hash);
            $buildingKey = $this->keys->rawBuildingKey($classKey, $seg, $lockSuffix);
            [$status, $seg, $blob] = $this->luaFetchRawBySeg($rawKey, $buildingKey, $seg);

            if ($status === 'hit') {
                return $this->rawResult('hit', $rawKey, $this->store->unserialize($blob), null);
            }

            if ($status === 'building') {
                return $this->rawResult('building', null, null, null);
            }

            return $this->rawResult('miss', $rawKey, null, $buildingKey, $wakeKey);
        }

        [$status, $seg, $blob] = $this->luaFetchVersionedRaw(
            $versionKeys,
            $scheduledKeys,
            $this->keys->rawPrefix($classKey) . $this->keys->tagSegment($tag),
            $this->keys->buildingPrefix($classKey),
            $hash,
            $lockSuffix
        );

        $rawKey = $this->keys->rawKey($classKey, $tag, $seg, $hash);

        if ($status === 'hit') {
            return $this->rawResult('hit', $rawKey, $this->store->unserialize($blob), null);
        }

        if ($status === 'building') {
            return $this->rawResult('building', null, null, null);
        }

        $buildingKey = $this->keys->rawBuildingKey($classKey, $seg, $lockSuffix);

        return $this->rawResult('miss', $rawKey, null, $buildingKey, $wakeKey);
    }

    public function waitForBuild(string $modelClass, string $hash, array $depClasses = [], ?string $tag = null): ?array
    {
        $classKey = $this->keys->classKey($modelClass);
        $wakeHash = $depClasses !== [] ? $this->keys->rawBuildLockSuffix($tag, $hash) : $hash;
        $this->store->brpop($this->keys->wakePrefix($classKey) . $wakeHash, $this->stampedeWaitMs / 1000.0);

        $result = $depClasses !== []
            ? $this->getRawCache($modelClass, $depClasses, $hash, $tag)
            : $this->getModelsFromQuery($modelClass, $hash, $tag);

        return match ($result['status']) {
            'building' => null,
            default => $result,
        };
    }

    // -------------------------------------------------------------------------
    // High-level cache writes
    // -------------------------------------------------------------------------

    public function storeThroughResult(string $key, array $payload, string $relatedClass, array $modelAttrs): void
    {
        $this->store->set($key, $payload, $this->queryTtl);
        $this->cacheModelAttrs($relatedClass, $modelAttrs);
    }

    public function storePivotResult(array $pivotEntriesByKey, string $relatedClass, array $modelAttrs): void
    {
        $this->store->setMany($pivotEntriesByKey, $this->queryTtl);
        $this->cacheModelAttrs($relatedClass, $modelAttrs);
    }

    public function storeQueryAggregate(string $key, mixed $value, ?int $ttl = null): void
    {
        $this->store->set($key, [$value], $ttl ?? $this->queryTtl);
    }

    public function storeQueryIds(string $key, array $ids, ?int $ttl = null, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = []): void
    {
        $ids = array_map('strval', $ids);

        if (!empty($versionKeys)) {
            $this->storeQueryIdsCAS($key, $ids, $ttl ?? $this->queryTtl, $buildingKey, $versionKeys, $expectedVersions);

            return;
        }

        $this->store->setJson($key, $ids, $ttl ?? $this->queryTtl);

        if ($buildingKey !== null) {
            $this->store->delete($buildingKey);
        }
    }

    public function storeRawResult(string $key, array $blob, ?string $buildingKey, ?int $ttl, ?string $wakeKey = null): void
    {
        $this->store->set($key, $blob, $ttl ?? $this->queryTtl);

        if ($buildingKey !== null) {
            $this->store->releaseBuilding($buildingKey, $wakeKey ?? $this->keys->buildingToWakeKey($buildingKey));
        }
    }

    // -------------------------------------------------------------------------
    // Private — write internals
    // -------------------------------------------------------------------------

    private function cacheModelAttrs(string $modelClass, array $modelAttrs): void
    {
        if (empty($modelAttrs)) {
            return;
        }

        $classKey = $this->keys->classKey($modelClass);
        $modelVersion = $this->currentVersion($modelClass);

        $attrsByKey = [];
        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey[$this->keys->modelPrefix($classKey) . $id] = $attrs;
        }

        $this->store->setManyTrackedIfVersion(
            $attrsByKey,
            $this->ttl,
            $this->keys->membersKey($classKey),
            $this->keys->verKey($classKey),
            $modelVersion
        );
    }

    private function storeQueryIdsCAS(string $key, array $ids, int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions): void
    {
        $this->store->eval(
            LuaScripts::get('store_query_cas'),
            array_merge($versionKeys, [$key, $buildingKey ?? '', $buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : '']),
            array_merge([(string) count($versionKeys), (string) $ttl], $expectedVersions, [json_encode($ids)])
        );
    }

    // -------------------------------------------------------------------------
    // Model loading (cache → hydrate → DB fallback)
    // -------------------------------------------------------------------------

    public function getModels(
        array $ids,
        string $modelClass,
        ?array $columns = null,
        ?array $raw = null,
        ?EloquentBuilder $missedQuery = null,
        bool $preserveQueryShape = true,
    ): array {
        if ($ids === []) {
            return [];
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $containedInQueryHit = $raw !== null;

        // arrays may come with sparse numeric keys, for example after array_unique().
        $ids = array_values($ids);

        $classKey = $this->keys->classKey($modelClass);

        if ($raw === null) {
            $keys = array_map(fn($id) => $this->keys->modelPrefix($classKey) . $id, $ids);
            $raw = $this->store->getMany($keys);
        }

        $projection = $columns !== null ? QueryInspector::normalizeProjection($columns) : null;
        ['hits' => $hits, 'missed' => $missed] = $this->hydrateModels($ids, $modelClass, $raw, $projection);

        if ($this->dispatchEvents && $hits !== []) {
            event(new ModelCacheHit($modelClass, array_keys($hits)));
        }

        if ($missed === []) {
            if (!$containedInQueryHit) {
                NormCacheCollector::recordModel('model hit', $modelClass, $ids, $debugbarStart);
            }

            return array_values($hits);
        }

        if ($this->dispatchEvents) {
            event(new ModelCacheMiss($modelClass, $missed));
        }

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

        NormCacheCollector::recordModel('model miss', $modelClass, $missed, $debugbarStart, [
            'hits' => array_keys($hits),
            'partial' => $hits !== [],
        ]);

        return $ordered;
    }

    public function hydrateRaw(array $blob, string $modelClass, bool $cached = true): array
    {
        $prototype = self::prototype($modelClass);
        $closure = self::hydratorClosure($modelClass);
        $fire = $this->fireRetrieved;
        $models = array_map(function ($attrs) use ($prototype, $closure, $fire) {
            $instance = clone $prototype;
            $closure($instance, $attrs, $fire);

            return $instance;
        }, $blob);

        if ($cached && $this->dispatchEvents && $models !== []) {
            $keys = array_values(array_filter(array_map(fn($m) => $m->getKey(), $models)));
            if ($keys !== []) {
                event(new ModelCacheHit($modelClass, $keys));
            }
        }

        return $models;
    }

    // -------------------------------------------------------------------------
    // Private — model hydration / DB fallback
    // -------------------------------------------------------------------------

    /** Returns (and lazily creates) the bound hydrator closure for $modelClass. */
    private static function hydratorClosure(string $modelClass): \Closure
    {
        return self::$hydratorClosures[$modelClass] ??= \Closure::bind(
            static function ($model, $attributes, $fire) {
                // Mirrors setRawAttributes($attributes, true): sets attributes,
                // syncs original, and clears both cast caches.
                $model->attributes = $attributes;
                $model->original = $attributes;
                $model->exists = true;
                $model->classCastCache = [];
                $model->attributeCastCache = [];

                if ($fire) {
                    $model->fireModelEvent('retrieved', false);
                }
            },
            null,
            $modelClass
        );
    }

    private function hydrateModels(array $ids, string $modelClass, array $raw, ?array $projection): array
    {
        $prototype = self::prototype($modelClass);

        $closure = self::hydratorClosure($modelClass);

        $fireRetrieved = $this->fireRetrieved;
        $hits = [];
        $missed = [];

        foreach ($ids as $i => $id) {
            $attrs = $raw[$i];

            if ($attrs === null || $attrs === false || !is_array($attrs)) {
                $missed[] = $id;

                continue;
            }

            if ($projection !== null) {
                $attrs = QueryInspector::projectAttributes($attrs, $projection);
            }

            $instance = clone $prototype;
            $closure($instance, $attrs, $fireRetrieved);
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
        $modelVersion = $this->currentVersion($modelClass);
        $prototype = self::prototype($modelClass);
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
        $deletedAtCol = self::deletedAtColumn($modelClass);

        foreach ($loaded as $id => $model) {
            $attrs = $model->getRawOriginal();

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if (!$isTrashed) {
                $attrsByKey[$this->keys->modelPrefix($classKey) . $id] = $attrs;
            }

            if ($projection !== null) {
                $model->setRawAttributes(QueryInspector::projectAttributes($attrs, $projection), true);
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

    // -------------------------------------------------------------------------
    // Private — Lua scripts
    // -------------------------------------------------------------------------

    private function luaFetchVersionedQuery(string $classKey, string $hash, int $nowMs, ?string $tag = null): mixed
    {
        return $this->store->eval(LuaScripts::get('fetch_versioned_query'), [
            $this->keys->verKey($classKey),
            $this->keys->scheduledKey($classKey),
            $this->keys->queryPrefix($classKey, $tag),
            $this->keys->modelPrefix($classKey),
            $this->keys->buildingPrefix($classKey),
        ], [$hash, $nowMs, $this->buildingLockTtl, $this->staleVersionDepth]);
    }

    private function luaFetchVersionedCache(array $versionKeys, array $scheduledKeys, string $keyPrefix, string $hash): array
    {
        $result = $this->store->eval(
            LuaScripts::get('fetch_versioned_cache'),
            array_merge($versionKeys, $scheduledKeys, [$keyPrefix]),
            [$hash, (int) floor(microtime(true) * 1000)]
        );

        return [(string) ($result[0] ?? ''), $result[1] ?? false];
    }

    private function luaFetchVersionedPivotCache(string $parentKey, string $relatedKey, string $relation, string $constraintHash, array $parentIds): array
    {
        $result = $this->store->eval(LuaScripts::get('fetch_versioned_pivot'), [
            $this->keys->verKey($parentKey),
            $this->keys->verKey($relatedKey),
            $this->keys->scheduledKey($parentKey),
            $this->keys->scheduledKey($relatedKey),
            $this->keys->pivotBasePrefix($parentKey, $relatedKey),
        ], array_merge([$relation, $constraintHash, (string) (int) floor(microtime(true) * 1000)], $parentIds));

        return [(string) ($result[0] ?? ''), $result[1] ?? []];
    }

    private function luaFetchVersionWithCooldown(string $classKey, int $nowMs): mixed
    {
        return $this->store->eval(
            LuaScripts::get('fetch_version_with_cooldown'),
            [$this->keys->verKey($classKey), $this->keys->scheduledKey($classKey)],
            [$nowMs]
        );
    }

    private function luaFetchVersionedRaw(array $versionKeys, array $scheduledKeys, string $rawPrefix, string $buildingPrefix, string $hash, string $lockSuffix): array
    {
        $result = $this->store->eval(
            LuaScripts::get('fetch_versioned_raw'),
            array_merge($versionKeys, $scheduledKeys, [$rawPrefix, $buildingPrefix]),
            [$hash, $lockSuffix, (string) $this->buildingLockTtl, (string) (int) floor(microtime(true) * 1000)]
        );

        return [$result[0] ?? 'building', (string) ($result[1] ?? ''), $result[2] ?? null];
    }

    private function luaFetchRawBySeg(string $rawKey, string $buildingKey, string $seg): array
    {
        $result = $this->store->eval(
            LuaScripts::get('fetch_raw_by_seg'),
            [$rawKey, $buildingKey],
            [$seg, (string) $this->buildingLockTtl]
        );

        return [$result[0] ?? 'building', (string) ($result[1] ?? $seg), $result[2] ?? null];
    }

    // -------------------------------------------------------------------------
    // Cluster-safe version resolution
    // -------------------------------------------------------------------------

    // One Lua call per unique key — each touches a single slot, safe in cluster.
    private function resolveVersionMap(array $versionKeys, array $scheduledKeys, int $nowMs): array
    {
        $script = LuaScripts::get('fetch_version_with_cooldown');
        $map = [];
        foreach ($versionKeys as $i => $verKey) {
            if (!array_key_exists($verKey, $map)) {
                $map[$verKey] = (string) ($this->store->eval($script, [$verKey, $scheduledKeys[$i]], [(string) $nowMs]) ?? '0');
            }
        }

        return $map;
    }

    private function resolveDepVersionsSeg(array $versionKeys, array $scheduledKeys, int $nowMs): string
    {
        $map = $this->resolveVersionMap($versionKeys, $scheduledKeys, $nowMs);

        return implode(':', array_map(fn($k) => 'v' . $map[$k], $versionKeys));
    }

    private function clusterFetchVersionedCache(array $versionKeys, array $scheduledKeys, string $keyPrefix, string $hash): array
    {
        $seg = $this->resolveDepVersionsSeg($versionKeys, $scheduledKeys, (int) floor(microtime(true) * 1000));

        return [$seg, $this->store->get($this->keys->versionedKey($keyPrefix, $seg, $hash))];
    }

    // -------------------------------------------------------------------------
    // Key building
    // -------------------------------------------------------------------------

    public function classKey(string $class): string
    {
        return $this->keys->classKey($class);
    }

    // -------------------------------------------------------------------------
    // Private — model metadata
    // -------------------------------------------------------------------------

    private static function prototype(string $modelClass): Model
    {
        return CacheKeyBuilder::prototypeFor($modelClass);
    }

    private static function deletedAtColumn(string $modelClass): ?string
    {
        return self::$deletedAtColumns[$modelClass] ??= method_exists(self::prototype($modelClass), 'getDeletedAtColumn')
            ? self::prototype($modelClass)->getDeletedAtColumn()
            : null;
    }

    // -------------------------------------------------------------------------
    // Infrastructure
    // -------------------------------------------------------------------------

    public function currentVersion(string $modelClass): int
    {
        return $this->normalizeVersion($this->resolveCurrentVersion($this->keys->classKey($modelClass)));
    }

    public function fallback(\Exception $e): void
    {
        if (!$this->fallbackEnabled) {
            throw $e;
        }

        report($e);
        $this->disable();
    }

    protected function handle(callable $operation): void
    {
        if (!$this->enabled) {
            return;
        }

        try {
            $operation();
        } catch (\Exception $e) {
            $this->fallback($e);
        }
    }

    private function normalizeVersion(mixed $value = null): int
    {
        return $value !== null ? (int) $value : 0;
    }

    private function rawResult(string $status, ?string $key, mixed $blob, ?string $buildingKey, ?string $wakeKey = null): array
    {
        return [
            'status' => $status,
            'key' => $key,
            'blob' => $blob,
            'buildingKey' => $buildingKey,
            'wakeKey' => $wakeKey,
        ];
    }

    private function queryResult(string $status, ?string $key, ?array $ids, ?array $models, ?string $buildingKey, array $versionKeys = [], array $expectedVersions = []): array
    {
        return [
            'status' => $status,
            'key' => $key,
            'ids' => $ids,
            'models' => $models,
            'buildingKey' => $buildingKey,
            'versionKeys' => $versionKeys,
            'expectedVersions' => $expectedVersions,
        ];
    }
}
