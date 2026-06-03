<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use NormCache\Support\AttributeProjector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Traits\HandlesInvalidation;

class CacheManager
{
    use HandlesInvalidation;

    private static array $hydratorClosures = [];

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
    // Configuration
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

    public function isSlotting(): bool
    {
        return $this->slotting;
    }

    public function enable(): void
    {
        $this->enabled = true;
    }

    public function disable(): void
    {
        $this->enabled = false;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public function getStore(): RedisStore
    {
        return $this->store;
    }

    public function classKey(string $class): string
    {
        return $this->keys->classKey($class);
    }

    public function tableKey(string $connectionName, string $table): string
    {
        return $this->keys->tableKey($connectionName, $table);
    }

    public function currentVersion(string $modelClass): int
    {
        return $this->normalizeVersion($this->resolveCurrentVersion($this->keys->classKey($modelClass)));
    }

    public function currentTableVersion(string $connectionName, string $table): int
    {
        return $this->normalizeVersion($this->resolveCurrentVersion($this->keys->tableKey($connectionName, $table)));
    }

    // -------------------------------------------------------------------------
    // Reads
    // -------------------------------------------------------------------------

    public function getModelsFromQuery(string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = []): array
    {
        $classKey = $this->keys->classKey($modelClass);

        if (empty($depClasses) && empty($depTableKeys)) {
            $lockToken = $this->buildLockToken();
            $result = $this->luaFetchVersionedQuery($classKey, $hash, (int) floor(microtime(true) * 1000), $tag, $lockToken);

            $luaStatus = $result[0] ?? null;
            $version = $this->normalizeVersion($result[1]);
            $queryKey = $this->keys->queryKey($classKey, $tag, $version, $hash);

            $deserialize = fn($r) => $this->store->unserializeMany($r);

            return match ($luaStatus) {
                'hit' => $this->queryResult('hit', $queryKey, $result[2], $deserialize($result[3]), null),
                'stale' => $this->queryResult('stale', null, $result[2], $deserialize($result[3]), null),
                'empty' => $this->queryResult('empty', $queryKey, [], [], null),
                'miss' => $this->queryResult('miss', $queryKey, null, null, $this->keys->buildingPrefix($classKey) . $hash, [$this->keys->verKey($classKey)], [(string) $version], (string) ($result[2] ?? $lockToken)),
                'building' => $this->queryResult('building', null, null, null, null),
                'corrupt' => $this->queryResult('miss', $queryKey, null, null, null, [$this->keys->verKey($classKey)], [(string) $version], null),
                default => $this->queryResult('miss', $queryKey, null, null, null),
            };
        }

        // Multi-dependency path
        $versionKeys = $this->keys->depVersionKeys($classKey, $depClasses, $depTableKeys);
        $scheduledKeys = $this->keys->depScheduledKeys($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->keys->queryPrefix($classKey, $tag);

        $lockToken = $this->buildLockToken();
        $result = $this->luaFetchMultiVersionedQuery($versionKeys, $scheduledKeys, $queryPrefix, $this->keys->modelPrefix($classKey), $this->keys->buildingPrefix($classKey), $hash, (int) floor(microtime(true) * 1000), $lockToken);

        $luaStatus = $result[0] ?? null;
        $seg = (string) ($result[1] ?? '');
        $queryKey = $queryPrefix . $seg . ':' . $hash;

        $deserialize = fn($r) => $this->store->unserializeMany($r);

        return match ($luaStatus) {
            'hit' => $this->queryResult('hit', $queryKey, $result[2], $deserialize($result[3]), null),
            'empty' => $this->queryResult('empty', $queryKey, [], [], null),
            'miss' => $this->queryResult('miss', $queryKey, null, null, $this->keys->buildingPrefix($classKey) . $seg . ':' . $hash, $versionKeys, $this->keys->versionsFromSegment($seg), (string) ($result[2] ?? $lockToken)),
            'building' => $this->queryResult('building', null, null, null, null),
            'corrupt' => $this->queryResult('miss', $queryKey, null, null, null, $versionKeys, $this->keys->versionsFromSegment($seg), null),
            default => $this->queryResult('miss', $queryKey, null, null, null),
        };
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc', ?string $pivotTableKey = null): array
    {
        $parentKey = $this->keys->classKey($parentClass);
        $relatedKey = $this->keys->classKey($relatedClass);
        $versionKeys = $this->keys->depVersionKeys($relatedKey, [], [$pivotTableKey ?? $parentKey]);
        $scheduledKeys = $this->keys->depScheduledKeys($relatedKey, [], [$pivotTableKey ?? $parentKey]);

        if ($this->slotting) {
            $resolvedVersions = $this->resolveVersions($versionKeys, $scheduledKeys, (int) floor(microtime(true) * 1000));
            $seg = $this->keys->versionSegment($versionKeys, $resolvedVersions);
            $expectedVersions = $this->expectedVersions($versionKeys, $resolvedVersions);
            $pivotKeyBuilder = $this->keys;
            $pivotKeys = array_map(static fn($id) => $pivotKeyBuilder->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id), $parentIds);

            return [
                'seg' => $seg,
                'data' => array_combine($parentIds, $this->store->getMany($pivotKeys)),
                'versionKeys' => $versionKeys,
                'expectedVersions' => $expectedVersions,
            ];
        }

        [$seg, $payloads] = $this->luaFetchVersionedPivotCache($parentKey, $relatedKey, $relation, $constraintHash, $parentIds, $versionKeys, $scheduledKeys);

        return [
            'seg' => $seg,
            'data' => array_combine($parentIds, $this->store->unserializeMany($payloads)),
            'versionKeys' => $versionKeys,
            'expectedVersions' => $this->keys->versionsFromSegment($seg),
        ];
    }

    public function getResultCache(string $modelClass, array $depClasses, string $hash, ?string $tag = null, array $depTableKeys = [], string $namespace = CacheKeyBuilder::K_RESULT): array
    {
        $classKey = $this->keys->classKey($modelClass);
        $lockSuffix = $this->keys->resultBuildIdentityHash($tag, $hash);
        $versionKeys = $this->keys->depVersionKeys($classKey, $depClasses, $depTableKeys);
        $scheduledKeys = $this->keys->depScheduledKeys($classKey, $depClasses, $depTableKeys);

        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);

        if ($this->slotting) {
            $resolvedVersions = $this->resolveVersions($versionKeys, $scheduledKeys, (int) floor(microtime(true) * 1000));
            $seg = $this->keys->versionSegment($versionKeys, $resolvedVersions);
            $expectedVersions = $this->expectedVersions($versionKeys, $resolvedVersions);
            $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
            $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);
            $lockToken = $this->buildLockToken();

            $payload = $this->store->get($resultKey);
            if ($payload !== null) {
                return $this->resultCacheResult('hit', $resultKey, $payload, null, versionKeys: $versionKeys, expectedVersions: $expectedVersions);
            }

            if (!$this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
                return $this->resultCacheResult('building', null, null, null);
            }

            return $this->resultCacheResult('miss', $resultKey, null, $buildingKey, $wakeKey, $versionKeys, $expectedVersions, $lockToken);
        }

        $lockToken = $this->buildLockToken();
        [$status, $seg, $payload, $claimedToken] = $this->luaFetchVersionedResult(
            $versionKeys,
            $scheduledKeys,
            $this->keys->namespacedPrefix($namespace, $classKey, $tag),
            $this->keys->buildingPrefix($classKey),
            $hash,
            $lockSuffix,
            $lockToken
        );

        $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        if ($status === 'hit') {
            return $this->resultCacheResult('hit', $resultKey, $this->store->unserialize($payload), null, versionKeys: $versionKeys, expectedVersions: $expectedVersions);
        }

        if ($status === 'building') {
            return $this->resultCacheResult('building', null, null, null);
        }

        $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);

        return $this->resultCacheResult('miss', $resultKey, null, $buildingKey, $wakeKey, $versionKeys, $expectedVersions, (string) ($claimedToken ?? $lockToken));
    }

    public function waitForBuild(string $store, string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = [], string $namespace = CacheKeyBuilder::K_RESULT): ?array
    {
        $classKey = $this->keys->classKey($modelClass);
        $isResult = $store === 'result';
        $wakeHash = $isResult ? $this->keys->resultBuildIdentityHash($tag, $hash) : $hash;

        $this->store->brpop($this->keys->wakePrefix($classKey) . $wakeHash, $this->stampedeWaitMs / 1000.0);

        $result = $isResult
            ? $this->getResultCache($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace)
            : $this->getModelsFromQuery($modelClass, $hash, $tag, $depClasses, $depTableKeys);

        return match ($result['status']) {
            'building' => null,
            default => $result,
        };
    }

    // -------------------------------------------------------------------------
    // Loading
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

        $debugbarStart = CacheReporter::beginMeasure();

        $containedInQueryHit = $raw !== null;

        // arrays may come with sparse numeric keys, for example after array_unique().
        $ids = array_values($ids);

        $classKey = $this->keys->classKey($modelClass);

        if ($raw === null) {
            $prefix = $this->keys->modelPrefix($classKey);
            $keys = array_map(static fn($id) => $prefix . $id, $ids);
            $raw = $this->store->getMany($keys);
        }

        $projection = $columns !== null ? AttributeProjector::normalizeProjection($columns) : null;
        ['hits' => $hits, 'missed' => $missed] = $this->hydrateModels($ids, $modelClass, $raw, $projection);

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

    public function hydrateResult(array $payload, string $modelClass, bool $cached = true): array
    {
        $prototype = CacheKeyBuilder::prototype($modelClass);
        $closure = self::hydratorClosure($modelClass);
        $fire = $this->fireRetrieved;
        $models = array_map(static function ($attrs) use ($prototype, $closure, $fire) {
            $instance = clone $prototype;
            $closure($instance, $attrs, $fire);

            return $instance;
        }, $payload);

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

    // -------------------------------------------------------------------------
    // Storage
    // -------------------------------------------------------------------------

    public function storeQueryIds(string $key, array $ids, ?int $ttl = null, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): void
    {
        $ids = array_map('strval', $ids);

        if (!empty($versionKeys)) {
            $this->storeQueryIdsCAS($key, $ids, $ttl ?? $this->queryTtl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken);

            return;
        }

        if ($buildingKey === null) {
            return;
        }

        $this->store->storeRawAndRelease(
            $key,
            json_encode($ids),
            $ttl ?? $this->queryTtl,
            $buildingKey,
            $this->keys->buildingToWakeKey($buildingKey),
            $buildingToken
        );
    }

    public function storeVersionedResult(string $key, mixed $payload, ?int $ttl = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null): bool
    {
        $ttl ??= $this->queryTtl;

        if ($versionKeys === []) {
            return $this->store->storeSerializedAndRelease(
                $key,
                $payload,
                $ttl,
                $buildingKey,
                $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : null),
                $buildingToken
            );
        }

        if ($this->slotting) {
            if ($buildingKey !== null && $buildingToken !== null && $this->store->getRaw($buildingKey) !== $buildingToken) {
                return false;
            }

            $written = $this->versionsStillMatch($versionKeys, $expectedVersions);

            if ($written) {
                $this->store->set($key, $payload, $ttl);
            }

            if ($buildingKey !== null) {
                $this->store->releaseBuilding($buildingKey, $wakeKey ?? $this->keys->buildingToWakeKey($buildingKey), $buildingToken);
            }

            return $written;
        }

        return (bool) $this->store->eval(
            RedisScripts::get('store_if_versions_match_and_release'),
            array_merge($versionKeys, [$key, $buildingKey ?? '', $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : '')]),
            array_merge([(string) count($versionKeys), (string) $ttl], $expectedVersions, [$this->store->serialize($payload), $buildingToken ?? ''])
        );
    }

    public function storeResultCache(string $key, array $payload, ?string $buildingKey, ?int $ttl, ?string $wakeKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): bool
    {
        if ($versionKeys !== []) {
            return $this->storeVersionedResult($key, $payload, $ttl ?? $this->queryTtl, $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken);
        }

        return $this->store->storeSerializedAndRelease(
            $key,
            $payload,
            $ttl ?? $this->queryTtl,
            $buildingKey,
            $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : null),
            $buildingToken
        );
    }

    public function cacheModelAttrs(string $modelClass, array $modelAttrs): void
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

    // -------------------------------------------------------------------------
    // Flow
    // -------------------------------------------------------------------------
    public function rescue(callable $operation, callable $fallback): mixed
    {
        try {
            return $operation();
        } catch (\Throwable $e) {
            $this->fallback($e);
        }

        return $fallback();
    }

    public function attempt(callable $operation): bool
    {
        try {
            $operation();

            return true;
        } catch (\Throwable $e) {
            $this->fallback($e);

            return false;
        }
    }

    public function fallback(\Throwable $e): void
    {
        if (!$this->fallbackEnabled) {
            throw $e;
        }

        report($e);
        $this->disable();
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function luaFetchVersionedQuery(string $classKey, string $hash, int $nowMs, ?string $tag = null, string $lockToken = ''): mixed
    {
        return $this->store->eval(RedisScripts::get('fetch_versioned_query'), [
            $this->keys->verKey($classKey),
            $this->keys->scheduledKey($classKey),
            $this->keys->queryPrefix($classKey, $tag),
            $this->keys->modelPrefix($classKey),
            $this->keys->buildingPrefix($classKey),
        ], [$hash, $nowMs, $this->buildingLockTtl, $this->staleVersionDepth, $lockToken]);
    }

    private function luaFetchMultiVersionedQuery(array $versionKeys, array $scheduledKeys, string $queryPrefix, string $modelPrefix, string $buildingPrefix, string $hash, int $nowMs, string $lockToken): mixed
    {
        return $this->store->eval(RedisScripts::get('fetch_multi_versioned_query'), array_merge($versionKeys, $scheduledKeys, [$queryPrefix, $modelPrefix, $buildingPrefix]), [$hash, $nowMs, $this->buildingLockTtl, $lockToken]);
    }

    private function luaFetchVersionedPivotCache(string $parentKey, string $relatedKey, string $relation, string $constraintHash, array $parentIds, array $versionKeys, array $scheduledKeys): array
    {
        $result = $this->store->eval(
            RedisScripts::get('fetch_versioned_pivot'),
            array_merge($versionKeys, $scheduledKeys, [$this->keys->pivotBasePrefix($parentKey, $relatedKey)]),
            array_merge([$relation, $constraintHash, (string) (int) floor(microtime(true) * 1000)], $parentIds)
        );

        return [(string) ($result[0] ?? ''), $result[1] ?? []];
    }

    private function luaFetchVersionWithCooldown(string $classKey, int $nowMs): mixed
    {
        return $this->store->eval(
            RedisScripts::get('fetch_version_with_cooldown'),
            [$this->keys->verKey($classKey), $this->keys->scheduledKey($classKey)],
            [$nowMs]
        );
    }

    private function luaFetchVersionedResult(array $versionKeys, array $scheduledKeys, string $resultPrefix, string $buildingPrefix, string $hash, string $lockSuffix, string $lockToken): array
    {
        $result = $this->store->eval(
            RedisScripts::get('fetch_versioned_result'),
            array_merge($versionKeys, $scheduledKeys, [$resultPrefix, $buildingPrefix]),
            [$hash, $lockSuffix, (string) $this->buildingLockTtl, (string) (int) floor(microtime(true) * 1000), $lockToken]
        );

        return [$result[0] ?? 'building', (string) ($result[1] ?? ''), $result[2] ?? null, $result[3] ?? null];
    }

    private function resolveVersions(array $versionKeys, array $scheduledKeys, int $nowMs): array
    {
        $script = RedisScripts::get('fetch_version_with_cooldown');
        $map = [];
        foreach ($versionKeys as $i => $verKey) {
            if (!isset($map[$verKey])) {
                $map[$verKey] = (string) ($this->store->eval($script, [$verKey, $scheduledKeys[$i]], [(string) $nowMs]) ?? '0');
            }
        }

        return $map;
    }

    private function expectedVersions(array $versionKeys, array $resolvedVersions): array
    {
        return array_map(static fn($key) => $resolvedVersions[$key], $versionKeys);
    }

    private function versionsStillMatch(array $versionKeys, array $expectedVersions): bool
    {
        foreach ($this->store->getRawMany($versionKeys) as $i => $version) {
            if ((string) ($version ?? '0') !== (string) $expectedVersions[$i]) {
                return false;
            }
        }

        return true;
    }

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
        $prototype = CacheKeyBuilder::prototype($modelClass);
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
                $attrs = AttributeProjector::projectAttributes($attrs, $projection);
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

    private function storeQueryIdsCAS(string $key, array $ids, int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken): void
    {
        $this->store->eval(
            RedisScripts::get('store_if_versions_match_and_release'),
            array_merge($versionKeys, [$key, $buildingKey ?? '', $buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : '']),
            array_merge([(string) count($versionKeys), (string) $ttl], $expectedVersions, [json_encode($ids), $buildingToken ?? ''])
        );
    }

    private function buildLockToken(): string
    {
        return bin2hex(random_bytes(16));
    }

    private function normalizeVersion(mixed $value = null): int
    {
        return $value !== null ? (int) $value : 0;
    }

    private function resultCacheResult(string $status, ?string $key, mixed $payload, ?string $buildingKey, ?string $wakeKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): array
    {
        return [
            'status' => $status,
            'key' => $key,
            'payload' => $payload,
            'buildingKey' => $buildingKey,
            'buildingToken' => $buildingToken,
            'wakeKey' => $wakeKey,
            'versionKeys' => $versionKeys,
            'expectedVersions' => $expectedVersions,
        ];
    }

    private function queryResult(string $status, ?string $key, ?array $ids, ?array $models, ?string $buildingKey, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): array
    {
        return [
            'status' => $status,
            'key' => $key,
            'ids' => $ids,
            'models' => $models,
            'buildingKey' => $buildingKey,
            'buildingToken' => $buildingToken,
            'versionKeys' => $versionKeys,
            'expectedVersions' => $expectedVersions,
        ];
    }
}
