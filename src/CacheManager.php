<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Support\QueryInspector;
use NormCache\Support\RedisStore;

class CacheManager
{
    private static array $classKeyCache = [];

    private static array $prototypes = [];

    private static array $hydratorClosures = [];

    private static array $deletedAtColumns = [];

    /** @var array<string, array<string, true>> */
    private array $flushQueue = [];

    /** @var array<string, int> */
    private array $versionLocal = [];

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
        private int $buildingLockTtl = 30,
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

    public function getModelsFromQuery(string $modelClass, string $hash): array
    {
        $classKey = $this->classKey($modelClass);

        $result = $this->luaFetchVersionedQuery($classKey, $hash, (int) floor(microtime(true) * 1000));

        if (!is_array($result)) {
            $version = $this->currentVersion($modelClass);

            return $this->queryResult(
                "query:{{$classKey}}:v{$version}:{$hash}",
                null,
                null,
                "building:{{$classKey}}:{$hash}"
            );
        }

        $status = $result[0] ?? null;
        $version = $this->normalizeVersion($result[1]);
        $this->versionLocal[$classKey] = $version;
        $queryKey = "query:{{$classKey}}:v{$version}:{$hash}";

        $deserialize = fn($r) => $this->store->unserializeMany($r);

        return match ($status) {
            'hit' => $this->queryResult($queryKey, $result[2], $deserialize($result[3]), null),
            'stale' => $this->queryResult(null, $result[2], $deserialize($result[3]), null),
            'empty' => $this->queryResult($queryKey, [], [], null),
            'miss' => $this->queryResult($queryKey, null, null, "building:{{$classKey}}:{$hash}", ["ver:{{$classKey}}:"], [(string) $version]),
            'building' => $this->queryResult(null, null, null, null),
            default => $this->queryResult($queryKey, null, null, null),
        };
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash, array $depClasses = []): array
    {
        $classKey = $this->classKey($modelClass);
        [$classKeys, $versionKeys] = $this->versionKeyData($classKey, $depClasses);

        [$versions, $rawData] = $this->luaFetchVersionedCache($versionKeys, "{$namespace}:{{$classKey}}:", $hash);

        foreach ($classKeys as $i => $key) {
            $this->versionLocal[$key] = $this->normalizeVersion($versions[$i]);
        }

        $seg = implode(':', array_map(fn($v) => 'v' . $v, $versions));
        $key = "{$namespace}:{{$classKey}}:{$seg}:{$hash}";

        return [
            'key' => $key,
            'data' => $rawData !== false ? $this->store->unserialize($rawData) : null,
            'version' => $this->normalizeVersion($versions[0]),
        ];
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->classKey($relatedClass);
        $throughKey = $this->classKey($throughClass);

        if (isset($this->versionLocal[$relatedKey], $this->versionLocal[$throughKey])) {
            $key = "through:{{$relatedKey}}:{$throughKey}:v{$this->versionLocal[$relatedKey]}:v{$this->versionLocal[$throughKey]}:{$hash}";
            $data = $this->store->get($key);

            return ['key' => $key, 'data' => $data];
        }

        [$versions, $rawData] = $this->luaFetchVersionedCache(
            ["ver:{{$relatedKey}}:", "ver:{{$throughKey}}:"],
            "through:{{$relatedKey}}:{$throughKey}:",
            $hash
        );

        $this->versionLocal[$relatedKey] = $this->normalizeVersion($versions[0]);
        $this->versionLocal[$throughKey] = $this->normalizeVersion($versions[1]);

        $key = "through:{{$relatedKey}}:{$throughKey}:v{$versions[0]}:v{$versions[1]}:{$hash}";

        return ['key' => $key, 'data' => $rawData !== false ? $this->store->unserialize($rawData) : null];
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc'): array
    {
        $parentKey = $this->classKey($parentClass);
        $relatedKey = $this->classKey($relatedClass);

        if (isset($this->versionLocal[$parentKey], $this->versionLocal[$relatedKey])) {
            $parentVersion = $this->versionLocal[$parentKey];
            $relatedVersion = $this->versionLocal[$relatedKey];
            $prefix = "pivot:{{$parentKey}}:{$relatedKey}:{$relation}:{$constraintHash}:v{$parentVersion}:v{$relatedVersion}:";
            $keys = array_map(fn($id) => $prefix . $id, $parentIds);

            return [
                'parentVersion' => $parentVersion,
                'relatedVersion' => $relatedVersion,
                'data' => array_combine($parentIds, $this->store->getMany($keys)),
            ];
        }

        [$versions, $rawDataArray] = $this->luaFetchVersionedPivotCache($parentKey, $relatedKey, $relation, $constraintHash, $parentIds);

        $this->versionLocal[$parentKey] = $this->normalizeVersion($versions[0]);
        $this->versionLocal[$relatedKey] = $this->normalizeVersion($versions[1]);

        return [
            'parentVersion' => $this->normalizeVersion($versions[0]),
            'relatedVersion' => $this->normalizeVersion($versions[1]),
            'data' => array_combine($parentIds, $this->store->unserializeMany($rawDataArray)),
        ];
    }

    public function getRelationAggregates(array $keys): array
    {
        return $this->store->getMany($keys);
    }

    public function getQueryAggregate(string $key): mixed
    {
        $cached = $this->store->get($key);

        return $cached !== null ? $cached[0] : null;
    }

    public function getQueryWithDeps(string $modelClass, array $depClasses, string $hash): array
    {
        $classKey = $this->classKey($modelClass);
        [$classKeys, $versionKeys] = $this->versionKeyData($classKey, $depClasses);

        $result = $this->luaFetchQueryWithDeps($classKey, $versionKeys, $hash);
        [$status, $versions] = $result;

        foreach ($classKeys as $i => $key) {
            $this->versionLocal[$key] = $this->normalizeVersion($versions[$i]);
        }

        $seg = implode(':', array_map(fn($v) => 'v' . $v, $versions ?: array_fill(0, count($versionKeys), 0)));
        $key = "query:{{$classKey}}:{$seg}:{$hash}";
        $buildingKey = "building:{{$classKey}}:{$seg}:{$hash}";

        $expectedVersions = array_map(fn($v) => (string) $this->normalizeVersion($v), $versions ?: array_fill(0, count($versionKeys), null));

        return match ($status) {
            'hit' => $this->queryResult($key, $result[2], $this->store->unserializeMany($result[3]), null),
            'empty' => $this->queryResult($key, [], [], null),
            'miss' => $this->queryResult($key, null, null, $buildingKey, $versionKeys, $expectedVersions),
            'building' => $this->queryResult(null, null, null, null),
            default => $this->queryResult($key, null, null, null),
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

    private function storeQueryIdsCAS(string $key, array $ids, int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions): void
    {
        // KEYS[1..n]   = version keys to check
        // KEYS[n+1]    = query key to write
        // KEYS[n+2]    = building key to always delete ('' to skip)
        // ARGV[1]      = n (number of version keys)
        // ARGV[2]      = TTL
        // ARGV[3..n+2] = expected version strings
        // ARGV[n+3]    = JSON payload
        $script = <<<'LUA'
            local n = tonumber(ARGV[1])
            for i = 1, n do
                local current = redis.call('GET', KEYS[i]) or '0'
                if current ~= ARGV[2 + i] then
                    if KEYS[n+2] ~= '' then redis.call('DEL', KEYS[n+2]) end
                    return 0
                end
            end
            redis.call('SETEX', KEYS[n+1], tonumber(ARGV[2]), ARGV[n+3])
            if KEYS[n+2] ~= '' then redis.call('DEL', KEYS[n+2]) end
            return 1
        LUA;

        $this->store->eval(
            $script,
            array_merge($versionKeys, [$key, $buildingKey ?? '']),
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
        $modelVersion = $this->currentVersion($modelClass);

        if ($raw === null) {
            $keys = array_map(fn($id) => "model:{{$classKey}}:" . $id, $ids);
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
            $modelVersion,
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
                $this->store->prefix("members:model:{{$classKey}}")
            );
        });
    }

    public function forceFlushModel(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);
        $this->doInvalidateVersion($modelClass);

        $memberKey = $this->store->prefix("members:model:{{$classKey}}");
        $this->store->sscanAndFlushSet($memberKey);
    }

    public function flushAll(): int
    {
        return $this->store->flushByPatterns([
            'query:*', 'model:*', 'members:model:*', 'ver:*',
            'agg:*', 'count:*', 'scalar:*', 'pivot:*', 'through:*', 'scheduled:*', 'building:*',
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

    public function flushVersionLocal(): void
    {
        $this->versionLocal = [];
    }

    // -------------------------------------------------------------------------
    // Private — invalidation internals
    // -------------------------------------------------------------------------

    private function doInvalidateVersion(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        if ($this->cooldown <= 0) {
            $this->versionLocal[$classKey] = $this->store->increment("ver:{{$classKey}}:");

            return;
        }

        $this->resolveCurrentVersion($classKey);
        $this->scheduleInvalidation($classKey);
    }

    private function resolveCurrentVersion(string $classKey): string|int|null
    {
        if ($this->cooldown <= 0) {
            return $this->store->getRaw("ver:{{$classKey}}:");
        }

        return $this->luaFetchVersionWithCooldown($classKey, (int) floor(microtime(true) * 1000));
    }

    private function scheduleInvalidation(string $classKey): void
    {
        $dueAtMs = (int) floor(microtime(true) * 1000) + ($this->cooldown * 1000);

        $this->store->setNx("scheduled:{{$classKey}}:", (string) $dueAtMs);
    }

    private function queueModelFlush(string $connectionName, string $modelClass): void
    {
        $this->flushQueue[$connectionName][$modelClass] = true;
    }

    // -------------------------------------------------------------------------
    // Private — model hydration / DB fallback
    // -------------------------------------------------------------------------

    private function hydrateModels(array $ids, string $modelClass, array $raw, ?array $projection): array
    {
        $prototype = self::prototype($modelClass);

        $closure = self::$hydratorClosures[$modelClass] ??= \Closure::bind(
            static function ($model, $attributes, $fire) {
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
        int $modelVersion,
    ): array {
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
                $attrsByKey["model:{{$classKey}}:$id"] = $attrs;
            }

            if ($projection !== null) {
                $model->setRawAttributes(QueryInspector::projectAttributes($attrs, $projection), true);
            }
        }

        if ($attrsByKey !== []) {
            $this->store->setManyTrackedIfVersion(
                $attrsByKey,
                $this->ttl,
                "members:model:{{$classKey}}",
                "ver:{{$classKey}}:",
                $modelVersion
            );
        }

        return $loaded->all();
    }

    private function prepareMissedQuery(string $modelClass, ?CacheableBuilder $missedQuery, bool $preserveQueryShape): EloquentBuilder
    {
        if ($missedQuery === null) {
            $builder = $modelClass::query();
            if ($builder instanceof CacheableBuilder) {
                return $builder->withoutCache();
            }

            return $builder;
        }

        $query = clone $missedQuery;
        $query->withoutCache();
        $query->setEagerLoads([]);
        $base = $query->getQuery()
            ->cloneWithout(['columns', 'orders', 'limit', 'offset'])
            ->cloneWithoutBindings(['select', 'order']);

        if (!$preserveQueryShape) {
            $base = $base
                ->cloneWithout(['joins', 'wheres'])
                ->cloneWithoutBindings(['join', 'where']);
        }

        $query->setQuery($base);

        return $query;
    }

    // -------------------------------------------------------------------------
    // Private — Lua scripts
    // -------------------------------------------------------------------------

    private function luaFetchVersionedQuery(string $classKey, string $hash, int $nowMs): mixed
    {
        $script = <<<'LUA'
            local function mget_models(model_prefix, ids)
                local keys = {}
                for i, id in ipairs(ids) do keys[i] = model_prefix .. id end
                local results = {}
                for start = 1, #keys, 500 do
                    local stop = math.min(start + 499, #keys)
                    local chunk = {}
                    for i = start, stop do chunk[#chunk + 1] = keys[i] end
                    local values = redis.call('MGET', unpack(chunk))
                    for i = 1, #values do results[#results + 1] = values[i] end
                end
                return results
            end

            local function serve_stale(ver, hash, query_prefix, model_prefix)
                local ver_num = tonumber(ver)
                for i = 1, 3 do
                    local stale_ver = ver_num - i
                    if stale_ver < 0 then break end
                    local stale_raw = redis.call('GET', query_prefix .. tostring(stale_ver) .. ':' .. hash)
                    if stale_raw then
                        local ok, ids = pcall(cjson.decode, stale_raw)
                        if ok and type(ids) == 'table' and #ids > 0 then
                            return {'stale', ver, ids, mget_models(model_prefix, ids)}
                        end
                    end
                end
                return nil
            end

            local now = tonumber(ARGV[2])
            local due_at = redis.call('GET', KEYS[2])
            local ver = redis.call('GET', KEYS[1])
            if not ver then ver = '0' end
            if due_at then
                local due_at_num = tonumber(due_at)
                if due_at_num and due_at_num <= now then
                    redis.call('DEL', KEYS[2])
                    ver = tostring(redis.call('INCR', KEYS[1]))
                elseif not due_at_num then
                    redis.call('DEL', KEYS[2])
                end
            end

            local query_key = KEYS[3] .. ver .. ':' .. ARGV[1]
            local ids_raw = redis.call('GET', query_key)
            if not ids_raw then
                local building_key = KEYS[5] .. ARGV[1]
                local claimed = redis.call('SET', building_key, '1', 'NX', 'EX', tonumber(ARGV[3]))
                if claimed then return {'miss', ver} end
                return serve_stale(ver, ARGV[1], KEYS[3], KEYS[4]) or {'building', ver}
            end

            local ok, ids = pcall(cjson.decode, ids_raw)
            if not ok or type(ids) ~= 'table' then
                redis.call('DEL', query_key)
                return {'corrupt', ver}
            end

            if #ids == 0 then return {'empty', ver} end

            return {'hit', ver, ids, mget_models(KEYS[4], ids)}
        LUA;

        return $this->store->eval($script, [
            "ver:{{$classKey}}:",
            "scheduled:{{$classKey}}:",
            "query:{{$classKey}}:v",
            "model:{{$classKey}}:",
            "building:{{$classKey}}:",
        ], [$hash, $nowMs, $this->buildingLockTtl]);
    }

    private function luaFetchQueryWithDeps(string $classKey, array $versionKeys, string $hash): mixed
    {
        $n = count($versionKeys);

        // KEYS[1..n]   = version keys for primary + sorted deps
        // KEYS[n+1]    = query key prefix  (query:{classKey}:)
        // KEYS[n+2]    = building key prefix (building:{classKey}:)
        // KEYS[n+3]    = model key prefix  (model:{classKey}:)
        // ARGV[1]      = hash
        // ARGV[2]      = number of version keys (n)
        //
        // Returns: {status, vers, [ids, models]}
        //   vers is always present so PHP can update versionLocal and rebuild the key.
        $script = <<<'LUA'
            local n = tonumber(ARGV[2])

            local ver_keys = {}
            for i = 1, n do ver_keys[i] = KEYS[i] end
            local vers = redis.call('MGET', unpack(ver_keys))
            for i = 1, n do if not vers[i] then vers[i] = '0' end end

            local seg = 'v' .. vers[1]
            for i = 2, n do seg = seg .. ':v' .. vers[i] end

            local query_key = KEYS[n+1] .. seg .. ':' .. ARGV[1]
            local building_key = KEYS[n+2] .. seg .. ':' .. ARGV[1]

            local ids_raw = redis.call('GET', query_key)
            if not ids_raw then
                local claimed = redis.call('SET', building_key, '1', 'NX', 'EX', tonumber(ARGV[3]))
                if not claimed then return {'building', vers} end
                return {'miss', vers}
            end

            local ok, ids = pcall(cjson.decode, ids_raw)
            if not ok or type(ids) ~= 'table' then
                redis.call('DEL', query_key)
                return {'corrupt', vers}
            end

            if #ids == 0 then return {'empty', vers} end

            local models = {}
            for start = 1, #ids, 500 do
                local stop = math.min(start + 499, #ids)
                local chunk = {}
                for i = start, stop do chunk[#chunk + 1] = KEYS[n+3] .. ids[i] end
                local values = redis.call('MGET', unpack(chunk))
                for i = 1, #values do models[#models + 1] = values[i] end
            end

            return {'hit', vers, ids, models}
        LUA;

        return $this->store->eval($script, array_merge(
            $versionKeys,
            ["query:{{$classKey}}:", "building:{{$classKey}}:", "model:{{$classKey}}:"]
        ), [$hash, $n, $this->buildingLockTtl]);
    }

    private function luaFetchVersionedCache(array $versionKeys, string $keyPrefix, string $hash): array
    {
        // KEYS[1..n]  = version keys
        // KEYS[n+1]   = key prefix (everything before the version segment)
        // ARGV[1]     = hash (suffix after the version segment)
        //
        // Returns: {vers, data_or_false}
        $script = <<<'LUA'
            local n = #KEYS - 1
            local ver_keys = {}
            for i = 1, n do ver_keys[i] = KEYS[i] end
            local vers = redis.call('MGET', unpack(ver_keys))
            for i = 1, n do if not vers[i] then vers[i] = '0' end end

            local seg = 'v' .. vers[1]
            for i = 2, n do seg = seg .. ':v' .. vers[i] end

            local data = redis.call('GET', KEYS[n+1] .. seg .. ':' .. ARGV[1])
            return {vers, data or false}
        LUA;

        $result = $this->store->eval($script, array_merge($versionKeys, [$keyPrefix]), [$hash]);

        return [$result[0] ?? [], $result[1] ?? false];
    }

    private function luaFetchVersionedPivotCache(string $parentKey, string $relatedKey, string $relation, string $constraintHash, array $parentIds): array
    {
        // KEYS[1] = ver:{parentKey}:, KEYS[2] = ver:{relatedKey}:, KEYS[3] = pivot:{parentKey}:{relatedKey}:
        // ARGV[1] = relation, ARGV[2] = constraintHash, ARGV[3..] = parentIds
        // Returns: {vers, [raw_data...]}
        $script = <<<'LUA'
            local n = #KEYS - 1
            local ver_keys = {}
            for i = 1, n do ver_keys[i] = KEYS[i] end
            local vers = redis.call('MGET', unpack(ver_keys))
            for i = 1, n do if not vers[i] then vers[i] = '0' end end

            local seg = 'v' .. vers[1]
            for i = 2, n do seg = seg .. ':v' .. vers[i] end

            local prefix = KEYS[n+1] .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. seg .. ':'
            local pivot_keys = {}
            for i = 3, #ARGV do pivot_keys[#pivot_keys + 1] = prefix .. ARGV[i] end
            local data = {}
            if #pivot_keys > 0 then data = redis.call('MGET', unpack(pivot_keys)) end
            return {vers, data}
        LUA;

        $result = $this->store->eval($script, [
            "ver:{{$parentKey}}:",
            "ver:{{$relatedKey}}:",
            "pivot:{{$parentKey}}:{$relatedKey}:",
        ], array_merge([$relation, $constraintHash], $parentIds));

        return [$result[0] ?? [], $result[1] ?? []];
    }

    private function luaFetchVersionWithCooldown(string $classKey, int $nowMs): mixed
    {
        $script = <<<'LUA'
            local now = tonumber(ARGV[1])
            local due_at = redis.call('GET', KEYS[2])
            if due_at then
                local due = tonumber(due_at)
                if due and due <= now then
                    redis.call('DEL', KEYS[2])
                    return tostring(redis.call('INCR', KEYS[1]))
                end
                if not due then redis.call('DEL', KEYS[2]) end
            end
            local ver = redis.call('GET', KEYS[1])
            return ver or '0'
        LUA;

        return $this->store->eval($script, ["ver:{{$classKey}}:", "scheduled:{{$classKey}}:"], [$nowMs]);
    }

    // -------------------------------------------------------------------------
    // Private — key building
    // -------------------------------------------------------------------------

    public function classKey(string $class): string
    {
        return self::$classKeyCache[$class] ??= $this->resolveClassKey($class);
    }

    private function modelKey(string $modelClass, string $id): string
    {
        return 'model:{' . $this->classKey($modelClass) . '}:' . $id;
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

    private function versionKeyData(string $classKey, array $depClasses): array
    {
        $classKeys = array_merge(
            [$classKey],
            array_map($this->classKey(...), $this->sortClassesByKey($depClasses))
        );

        $versionKeys = array_map(fn($key) => "ver:{{$key}}:", $classKeys);

        return [$classKeys, $versionKeys];
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
        $classKey = $this->classKey($modelClass);

        if (isset($this->versionLocal[$classKey])) {
            return $this->versionLocal[$classKey];
        }

        $value = $this->resolveCurrentVersion($classKey);

        return $this->versionLocal[$classKey] = $this->normalizeVersion($value);
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

    private function queryResult(?string $key, ?array $ids, ?array $models, ?string $buildingKey, array $versionKeys = [], array $expectedVersions = []): array
    {
        return [
            'key' => $key,
            'ids' => $ids,
            'models' => $models,
            'buildingKey' => $buildingKey,
            'versionKeys' => $versionKeys,
            'expectedVersions' => $expectedVersions,
        ];
    }

    private function cacheModelAttrs(string $modelClass, array $modelAttrs): void
    {
        if (empty($modelAttrs)) {
            return;
        }

        $classKey = $this->classKey($modelClass);
        $modelVersion = $this->currentVersion($modelClass);

        $attrsByKey = [];
        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey["model:{{$classKey}}:{$id}"] = $attrs;
        }

        $this->store->setManyTrackedIfVersion(
            $attrsByKey,
            $this->ttl,
            "members:model:{{$classKey}}",
            "ver:{{$classKey}}:",
            $modelVersion
        );
    }
}
