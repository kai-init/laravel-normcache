<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Support\LuaScripts;
use NormCache\Support\QueryInspector;
use NormCache\Support\RedisStore;

class CacheManager
{
    // -------------------------------------------------------------------------
    // Key-prefix constants — single source of truth for PHP and Lua
    // -------------------------------------------------------------------------

    public const K_VER = 'ver';

    public const K_SCHEDULED = 'scheduled';

    public const K_QUERY = 'query';

    public const K_MODEL = 'model';

    public const K_BUILDING = 'building';

    public const K_MEMBERS = 'members:model';

    public const K_AGG = 'agg';

    public const K_COUNT = 'count';

    public const K_SCALAR = 'scalar';

    public const K_PIVOT = 'pivot';

    public const K_THROUGH = 'through';

    public const K_WAKE = 'wake';

    public const K_RAW = 'raw';

    private static array $classKeyCache = [];

    private static array $prototypes = [];

    private static array $hydratorClosures = [];

    private static array $deletedAtColumns = [];

    /** @var array<string, array<string, true>> */
    private array $flushQueue = [];

    private RedisStore $store;

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
    ) {
        $this->store = new RedisStore($redisConnection, $keyPrefix, $cluster);
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

    public function isFallbackEnabled(): bool
    {
        return $this->fallbackEnabled;
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
        $classKey = $this->classKey($modelClass);

        $result = $this->luaFetchVersionedQuery($classKey, $hash, (int) floor(microtime(true) * 1000), $tag);

        $luaStatus = $result[0] ?? null;
        $version = $this->normalizeVersion($result[1]);
        $queryKey = $this->queryPrefix($classKey, $tag) . $version . ':' . $hash;

        $deserialize = fn($r) => $this->store->unserializeMany($r);

        return match ($luaStatus) {
            'hit' => $this->queryResult('hit', $queryKey, $result[2], $deserialize($result[3]), null),
            'stale' => $this->queryResult('stale', null, $result[2], $deserialize($result[3]), null),
            'empty' => $this->queryResult('empty', $queryKey, [], [], null),
            'miss' => $this->queryResult('miss', $queryKey, null, null, $this->buildingPrefix($classKey) . $hash, [$this->verKey($classKey)], [(string) $version]),
            'building' => $this->queryResult('building', null, null, null, null),
            default => $this->queryResult('miss', $queryKey, null, null, null),
        };
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash, array $depClasses = [], ?string $tag = null): array
    {
        $classKey = $this->classKey($modelClass);
        $versionKeys = $this->depVersionKeys($classKey, $depClasses);

        $tagSegment = $tag !== null ? $tag . ':' : '';
        [$seg, $blob] = $this->luaFetchVersionedCache($versionKeys, $namespace . ':{' . $classKey . '}:' . $tagSegment, $hash);

        return [
            'key' => "{$namespace}:{{$classKey}}:{$tagSegment}{$seg}:{$hash}",
            'data' => $blob !== false ? $this->store->unserialize($blob) : null,
        ];
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->classKey($relatedClass);
        $throughKey = $this->classKey($throughClass);

        [$seg, $blob] = $this->luaFetchVersionedCache(
            [$this->verKey($relatedKey), $this->verKey($throughKey)],
            self::K_THROUGH . ':{' . $relatedKey . '}:' . $throughKey . ':',
            $hash
        );

        $key = self::K_THROUGH . ':{' . $relatedKey . '}:' . $throughKey . ':' . $seg . ':' . $hash;

        return ['key' => $key, 'data' => $blob !== false ? $this->store->unserialize($blob) : null];
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc'): array
    {
        $parentKey = $this->classKey($parentClass);
        $relatedKey = $this->classKey($relatedClass);

        [$seg, $blobs] = $this->luaFetchVersionedPivotCache($parentKey, $relatedKey, $relation, $constraintHash, $parentIds);

        return [
            'seg' => $seg,
            'data' => array_combine($parentIds, $this->store->unserializeMany($blobs)),
        ];
    }

    public function fetchVersionedAggregates(string $keyPrefix, array $parentIds, array $specs): array
    {
        $keys = [$keyPrefix];
        $argv = [(string) count($parentIds), ...array_map('strval', $parentIds), (string) count($specs)];

        foreach ($specs as $spec) {
            $keys[] = $this->verKey($this->classKey($spec['relatedClass']));
            $keys[] = $this->verKey($this->classKey($spec['secondClass'] ?? $spec['relatedClass']));
            array_push($argv, $spec['staticSuffix'], $spec['secondLabel'] ?? '');
        }

        [$blobs, $suffixes] = $this->luaFetchVersionedAggregates($keys, $argv);

        return [
            'data' => $this->store->unserializeMany($blobs),
            'suffixes' => $suffixes,
        ];
    }

    public function getRawCache(string $modelClass, array $depClasses, string $hash, ?string $tag = null): array
    {
        $classKey = $this->classKey($modelClass);
        $tagSegment = $tag !== null ? $tag . ':' : '';

        [$status, $seg, $blob] = $this->luaFetchVersionedRaw(
            $this->depVersionKeys($classKey, $depClasses),
            $this->rawPrefix($classKey) . $tagSegment,
            $this->buildingPrefix($classKey),
            $hash
        );

        if ($status === 'building') {
            return $this->rawResult('building', null, null, null);
        }

        $key = $this->rawPrefix($classKey) . $tagSegment . $seg . ':' . $hash;

        return match ($status) {
            'hit' => $this->rawResult('hit', $key, $this->store->unserialize($blob), null),
            'miss' => $this->rawResult('miss', $key, null, $this->buildingPrefix($classKey) . $hash),
        };
    }

    public function waitForBuild(string $modelClass, string $hash, bool $returnOnMiss = true, array $depClasses = [], ?string $tag = null): ?array
    {
        $this->store->brpop($this->wakePrefix($this->classKey($modelClass)) . $hash, $this->stampedeWaitMs / 1000.0);

        $result = $depClasses !== []
            ? $this->getRawCache($modelClass, $depClasses, $hash, $tag)
            : $this->getModelsFromQuery($modelClass, $hash, $tag);

        return match ($result['status']) {
            'building' => null,
            'miss' => $returnOnMiss ? $result : $this->discardBuildAndReturnNull($result),
            default => $result,
        };
    }

    // -------------------------------------------------------------------------
    // High-level cache writes
    // -------------------------------------------------------------------------

    public function storeThroughResult(string $key, array $payload, string $relatedClass, array $modelAttrs): void
    {
        $this->store->set($key, $payload, $this->queryTtl);

        if (!empty($modelAttrs)) {
            $this->cacheModelAttrs($relatedClass, $modelAttrs);
        }
    }

    public function storePivotResult(array $pivotEntriesByKey, string $relatedClass, array $modelAttrs): void
    {
        $this->store->setMany($pivotEntriesByKey, $this->queryTtl);

        if (!empty($modelAttrs)) {
            $this->cacheModelAttrs($relatedClass, $modelAttrs);
        }
    }

    public function setRelationAggregates(array $entries): void
    {
        $this->store->setMany($entries, $this->queryTtl);
    }

    public function storeQueryAggregate(string $key, mixed $value, ?int $ttl = null): void
    {
        $this->store->set($key, [$value], $ttl ?? $this->queryTtl);
    }

    public function storeQueryIds(string $key, array $ids, ?int $ttl = null, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = []): void
    {
        if (!empty($versionKeys)) {
            $this->storeQueryIdsCAS($key, $ids, $ttl ?? $this->queryTtl, $buildingKey, $versionKeys, $expectedVersions);

            return;
        }

        $this->store->setJson($key, $ids, $ttl ?? $this->queryTtl);

        if ($buildingKey !== null) {
            $this->store->delete($buildingKey);
        }
    }

    public function storeRawResult(string $key, array $blob, ?string $buildingKey, ?int $ttl): void
    {
        $this->store->set($key, $blob, $ttl ?? $this->queryTtl);

        if ($buildingKey !== null) {
            $this->store->releaseBuilding($buildingKey, $this->buildingToWakeKey($buildingKey));
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

        $classKey = $this->classKey($modelClass);
        $modelVersion = $this->currentVersion($modelClass);

        $attrsByKey = [];
        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey[$this->modelPrefix($classKey) . $id] = $attrs;
        }

        $this->store->setManyTrackedIfVersion(
            $attrsByKey,
            $this->ttl,
            $this->membersKey($classKey),
            $this->verKey($classKey),
            $modelVersion
        );
    }

    private function discardBuildAndReturnNull(array $result): null
    {
        if ($result['buildingKey'] !== null) {
            $this->store->delete($result['buildingKey']);
        }

        return null;
    }

    private function storeQueryIdsCAS(string $key, array $ids, int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions): void
    {
        $this->store->eval(
            LuaScripts::get('store_query_cas'),
            array_merge($versionKeys, [$key, $buildingKey ?? '', $buildingKey !== null ? $this->buildingToWakeKey($buildingKey) : '']),
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

        $classKey = $this->classKey($modelClass);

        if ($raw === null) {
            $keys = array_map(fn($id) => $this->modelPrefix($classKey) . $id, $ids);
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
    // Invalidation
    // -------------------------------------------------------------------------

    public function invalidateVersion(Model $model): void
    {
        if (!$this->enabled) {
            return;
        }

        $conn = $model->getConnection()->getName();

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueModelFlush($conn, $model::class);

            return;
        }

        $this->handle(fn() => $this->doInvalidateVersion($model::class));
    }

    public function flushModel(Model|string $model): void
    {
        if (!$this->enabled) {
            return;
        }

        if (is_string($model)) {
            $this->handle(fn() => $this->forceFlushModel($model));

            return;
        }

        $conn = $model->getConnection()->getName();

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueModelFlush($conn, $model::class);

            return;
        }

        $this->handle(fn() => $this->forceFlushModel($model::class));
    }

    public function flushInstance(Model $model): void
    {
        if (!$this->enabled) {
            return;
        }

        $conn = $model->getConnection()->getName();
        $class = $model::class;
        $key = $this->modelKey($class, $model->getKey());

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueModelFlush($conn, $class);

            return;
        }

        $this->handle(function () use ($class, $key) {
            $classKey = $this->classKey($class);
            $this->doInvalidateVersion($class);
            $this->store->deleteFromSet(
                $this->store->prefix($key),
                $this->store->prefix($this->membersKey($classKey))
            );
        });
    }

    public function forceFlushModel(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);
        $this->doInvalidateVersion($modelClass);

        $this->store->sscanAndFlushSet($this->store->prefix($this->membersKey($classKey)));
    }

    public function flushAll(): int
    {
        return $this->store->flushByPatterns([
            self::K_QUERY . ':*',
            self::K_MODEL . ':*',
            self::K_MEMBERS . ':*',
            self::K_VER . ':*',
            self::K_AGG . ':*',
            self::K_COUNT . ':*',
            self::K_SCALAR . ':*',
            self::K_PIVOT . ':*',
            self::K_THROUGH . ':*',
            self::K_SCHEDULED . ':*',
            self::K_BUILDING . ':*',
            self::K_WAKE . ':*',
            self::K_RAW . ':*',
        ]);
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        $classKey = $this->classKey($modelClass);

        return $this->store->flushByPatterns([
            self::K_RAW . ':{' . $classKey . '}:' . $tag . ':*',
            self::K_QUERY . ':{' . $classKey . '}:' . $tag . ':*',
            self::K_COUNT . ':{' . $classKey . '}:' . $tag . ':*',
            self::K_SCALAR . ':{' . $classKey . '}:' . $tag . ':*',
        ]);
    }

    public function flushTagAcrossModels(string $tag): int
    {
        return $this->store->flushByPatterns([
            self::K_RAW . ':*:' . $tag . ':*',
            self::K_QUERY . ':*:' . $tag . ':*',
            self::K_COUNT . ':*:' . $tag . ':*',
            self::K_SCALAR . ':*:' . $tag . ':*',
        ]);
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connectionName = null): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            foreach ($modelClasses as $modelClass) {
                $this->queueModelFlush($connectionName, $modelClass);
            }

            return;
        }

        foreach ($modelClasses as $modelClass) {
            $this->doInvalidateVersion($modelClass);
        }
    }

    public function commitPending(string $connectionName): void
    {
        $flushes = array_keys($this->flushQueue[$connectionName] ?? []);

        unset($this->flushQueue[$connectionName]);

        if (empty($flushes) || !$this->enabled) {
            return;
        }

        $this->handle(function () use ($flushes) {
            foreach ($flushes as $modelClass) {
                $this->forceFlushModel($modelClass);
            }
        });
    }

    public function discardPending(string $connectionName): void
    {
        unset($this->flushQueue[$connectionName]);
    }

    public function discardAllPending(): void
    {
        $this->flushQueue = [];
    }

    // -------------------------------------------------------------------------
    // Private — invalidation internals
    // -------------------------------------------------------------------------

    private function doInvalidateVersion(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        if ($this->cooldown <= 0) {
            $this->store->increment($this->verKey($classKey));

            return;
        }

        $this->resolveCurrentVersion($classKey);
        $this->scheduleInvalidation($classKey);
    }

    private function resolveCurrentVersion(string $classKey): string|int|null
    {
        if ($this->cooldown <= 0) {
            return $this->store->getRaw($this->verKey($classKey));
        }

        return $this->luaFetchVersionWithCooldown($classKey, (int) floor(microtime(true) * 1000));
    }

    private function scheduleInvalidation(string $classKey): void
    {
        $dueAtMs = (int) floor(microtime(true) * 1000) + ($this->cooldown * 1000);

        $this->store->setNx($this->scheduledKey($classKey), (string) $dueAtMs);
    }

    private function queueModelFlush(string $connectionName, string $modelClass): void
    {
        $this->flushQueue[$connectionName][$modelClass] = true;
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
                $attrsByKey[$this->modelPrefix($classKey) . $id] = $attrs;
            }

            if ($projection !== null) {
                $model->setRawAttributes(QueryInspector::projectAttributes($attrs, $projection), true);
            }
        }

        if ($attrsByKey !== []) {
            $this->store->setManyTrackedIfVersion(
                $attrsByKey,
                $this->ttl,
                $this->membersKey($classKey),
                $this->verKey($classKey),
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

        return (new CacheableBuilder($base))
            ->setModel($missedQuery->getModel())
            ->withoutCache();
    }

    // -------------------------------------------------------------------------
    // Private — Lua scripts
    // -------------------------------------------------------------------------

    private function luaFetchVersionedQuery(string $classKey, string $hash, int $nowMs, ?string $tag = null): mixed
    {
        return $this->store->eval(LuaScripts::get('fetch_versioned_query'), [
            $this->verKey($classKey),
            $this->scheduledKey($classKey),
            $this->queryPrefix($classKey, $tag),
            $this->modelPrefix($classKey),
            $this->buildingPrefix($classKey),
        ], [$hash, $nowMs, $this->buildingLockTtl]);
    }

    private function luaFetchVersionedCache(array $versionKeys, string $keyPrefix, string $hash): array
    {
        $result = $this->store->eval(
            LuaScripts::get('fetch_versioned_cache'),
            array_merge($versionKeys, [$keyPrefix]),
            [$hash]
        );

        return [(string) ($result[0] ?? ''), $result[1] ?? false];
    }

    private function luaFetchVersionedPivotCache(string $parentKey, string $relatedKey, string $relation, string $constraintHash, array $parentIds): array
    {
        $result = $this->store->eval(LuaScripts::get('fetch_versioned_pivot'), [
            $this->verKey($parentKey),
            $this->verKey($relatedKey),
            self::K_PIVOT . ':{' . $parentKey . '}:' . $relatedKey . ':',
        ], array_merge([$relation, $constraintHash], $parentIds));

        return [(string) ($result[0] ?? ''), $result[1] ?? []];
    }

    private function luaFetchVersionWithCooldown(string $classKey, int $nowMs): mixed
    {
        return $this->store->eval(
            LuaScripts::get('fetch_version_with_cooldown'),
            [$this->verKey($classKey), $this->scheduledKey($classKey)],
            [$nowMs]
        );
    }

    private function luaFetchVersionedRaw(array $versionKeys, string $rawPrefix, string $buildingPrefix, string $hash): array
    {
        $result = $this->store->eval(
            LuaScripts::get('fetch_versioned_raw'),
            array_merge($versionKeys, [$rawPrefix, $buildingPrefix]),
            [$hash, (string) $this->buildingLockTtl]
        );

        return [$result[0] ?? 'building', (string) ($result[1] ?? ''), $result[2] ?? null];
    }

    private function luaFetchVersionedAggregates(array $keys, array $argv): array
    {
        $result = $this->store->eval(LuaScripts::get('fetch_versioned_aggregates'), $keys, $argv);

        return [(array) ($result[0] ?? []), (array) ($result[1] ?? [])];
    }

    // -------------------------------------------------------------------------
    // Key building
    // -------------------------------------------------------------------------

    public function classKey(string $class): string
    {
        return self::$classKeyCache[$class] ??= $this->resolveClassKey($class);
    }

    private function verKey(string $classKey): string
    {
        return self::K_VER . ':{' . $classKey . '}:';
    }

    private function scheduledKey(string $classKey): string
    {
        return self::K_SCHEDULED . ':{' . $classKey . '}:';
    }

    private function modelPrefix(string $classKey): string
    {
        return self::K_MODEL . ':{' . $classKey . '}:';
    }

    /** Prefix for single-version query keys: appended with "{version}:{hash}". */
    private function queryPrefix(string $classKey, ?string $tag = null): string
    {
        $base = self::K_QUERY . ':{' . $classKey . '}:';

        return $tag !== null ? $base . $tag . ':v' : $base . 'v';
    }

    private function rawPrefix(string $classKey): string
    {
        return self::K_RAW . ':{' . $classKey . '}:';
    }

    private function buildingPrefix(string $classKey): string
    {
        return self::K_BUILDING . ':{' . $classKey . '}:';
    }

    private function wakePrefix(string $classKey): string
    {
        return self::K_WAKE . ':{' . $classKey . '}:';
    }

    private function membersKey(string $classKey): string
    {
        return self::K_MEMBERS . ':{' . $classKey . '}';
    }

    private function modelKey(string $modelClass, string $id): string
    {
        return $this->modelPrefix($this->classKey($modelClass)) . $id;
    }

    private function resolveClassKey(string $class): string
    {
        $model = self::prototype($class);
        $connection = $model->getConnectionName() ?? DB::getDefaultConnection();

        return "{$connection}:{$model->getTable()}";
    }

    private function sortClassesByKey(array $classes): array
    {
        usort($classes, fn($a, $b) => strcmp($this->classKey($a), $this->classKey($b)));

        return $classes;
    }

    private function depVersionKeys(string $classKey, array $depClasses): array
    {
        $all = array_merge([$classKey], array_map($this->classKey(...), $this->sortClassesByKey($depClasses)));

        return array_map(fn($key) => $this->verKey($key), $all);
    }

    private function buildingToWakeKey(string $buildingKey): string
    {
        $classKeyEnd = strpos($buildingKey, '}:') + 2;

        return self::K_WAKE
            . substr($buildingKey, strlen(self::K_BUILDING), $classKeyEnd - strlen(self::K_BUILDING))
            . substr(strrchr($buildingKey, ':'), 1);
    }

    // -------------------------------------------------------------------------
    // Private — model metadata
    // -------------------------------------------------------------------------

    private static function prototype(string $modelClass): Model
    {
        return self::$prototypes[$modelClass] ??= new $modelClass;
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
        return $this->normalizeVersion($this->resolveCurrentVersion($this->classKey($modelClass)));
    }

    public function fallback(\Exception $e): void
    {
        if (!$this->fallbackEnabled) {
            throw $e;
        }

        report($e);
        $this->disable();
    }

    private function handle(callable $operation): void
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

    private function rawResult(string $status, ?string $key, mixed $blob, ?string $buildingKey): array
    {
        return [
            'status' => $status,
            'key' => $key,
            'blob' => $blob,
            'buildingKey' => $buildingKey,
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
