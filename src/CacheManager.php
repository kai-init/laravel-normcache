<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
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
        $script = <<<'LUA'
            local now = tonumber(ARGV[2])

            local due_at = redis.call('GET', KEYS[2])
            local ver = redis.call('GET', KEYS[1])
            if not ver then ver = '0' end

            local due_at_num = due_at and tonumber(due_at) or nil
            if due_at_num and due_at_num <= now then
                redis.call('DEL', KEYS[2])
                ver = tostring(redis.call('INCR', KEYS[1]))
            end

            local query_key = KEYS[3] .. ver .. ':' .. ARGV[1]
            local ids_raw = redis.call('GET', query_key)

            if not ids_raw then
                return {'miss', ver}
            end

            local ok, ids = pcall(cjson.decode, ids_raw)
            if not ok or type(ids) ~= 'table' then
                redis.call('DEL', query_key)
                return {'corrupt', ver}
            end

            if #ids == 0 then
                return {'empty', ver}
            end

            local model_keys = {}
            for i, id in ipairs(ids) do
                model_keys[i] = KEYS[4] .. id
            end

            local models = {}
            local chunk_size = 500

            for start = 1, #model_keys, chunk_size do
                local stop = math.min(start + chunk_size - 1, #model_keys)
                local chunk = {}

                for i = start, stop do
                    chunk[#chunk + 1] = model_keys[i]
                end

                local values = redis.call('MGET', unpack(chunk))

                for i = 1, #values do
                    models[#models + 1] = values[i]
                end
            end

            return {'hit', ver, ids, models}
        LUA;

        $result = $this->store->eval(
            $script,
            [
                "ver:{{$classKey}}:",
                "scheduled:{{$classKey}}:",
                "query:{{$classKey}}:v",
                "model:{{$classKey}}:",
            ],
            [$hash, (int) floor(microtime(true) * 1000)]
        );

        if (!is_array($result)) {
            $version = $this->currentVersion($modelClass);

            return ['key' => "query:{{$classKey}}:v{$version}:{$hash}", 'ids' => null, 'models' => null];
        }

        $status = $result[0] ?? null;
        $version = (int) ($result[1] ?? 0);
        $this->versionLocal[$classKey] = $version;
        $queryKey = "query:{{$classKey}}:v{$version}:{$hash}";

        if ($status === 'miss') {
            return ['key' => $queryKey, 'ids' => null, 'models' => null];
        }

        if ($status === 'corrupt') {
            return ['key' => $queryKey, 'ids' => null, 'models' => null];
        }

        if ($status === 'empty') {
            return ['key' => $queryKey, 'ids' => [], 'models' => []];
        }

        if ($status !== 'hit') {
            return ['key' => $queryKey, 'ids' => null, 'models' => null];
        }

        $models = $this->store->deserializeMany($result[3]);

        return ['key' => $queryKey, 'ids' => $result[2], 'models' => $models];
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash): array
    {
        $classKey = $this->classKey($modelClass);
        $version = $this->currentVersion($modelClass);
        $key = "{$namespace}:{{$classKey}}:v{$version}:{$hash}";

        return [
            'key' => $key,
            'data' => $this->store->get($key),
            'version' => $version,
        ];
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->classKey($relatedClass);
        $throughKey = $this->classKey($throughClass);
        $key = "through:{{$relatedKey}}:{$throughKey}:v{$this->currentVersion($relatedClass)}:v{$this->currentVersion($throughClass)}:{$hash}";
        $data = $this->store->get($key);

        if ($data !== null) {
            return ['key' => $key, 'data' => $data];
        }

        return ['key' => $key, 'data' => null];
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc'): array
    {
        $parentKey = $this->classKey($parentClass);
        $relatedKey = $this->classKey($relatedClass);
        $parentVersion = $this->currentVersion($parentClass);
        $relatedVersion = $this->currentVersion($relatedClass);
        $prefix = "pivot:{{$parentKey}}:{$relatedKey}:{$relation}:{$constraintHash}:v{$parentVersion}:v{$relatedVersion}:";
        $keys = array_map(fn($id) => $prefix . $id, $parentIds);

        return [
            'parentVersion' => $parentVersion,
            'relatedVersion' => $relatedVersion,
            'data' => array_combine($parentIds, $this->store->getMany($keys)),
        ];
    }

    public function getAggregates(array $keys): array
    {
        return $this->store->getMany($keys);
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

    public function setAggregates(array $entries): void
    {
        $this->store->setMany($entries, $this->queryTtl);
    }

    public function storeCount(string $key, int $count, ?int $ttl = null): void
    {
        $this->store->set($key, $count, $ttl ?? $this->queryTtl);
    }

    public function storeQueryIds(string $key, array $ids, ?int $ttl = null): void
    {
        $this->store->setJson($key, $ids, $ttl ?? $this->queryTtl);
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

        $classKey = $this->classKey($modelClass);

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
            $this->store->deleteFromSet(
                $this->store->prefix($key),
                $this->store->prefix("members:model:{{$classKey}}")
            );
            $this->doInvalidateVersion($class);
        });
    }

    public function forceFlushModel(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);
        $memberKey = $this->store->prefix("members:model:{{$classKey}}");
        $keys = $this->store->smembers($memberKey);

        if (!empty($keys)) {
            $this->store->asyncDel($keys);
        }

        $this->store->delete("members:model:{{$classKey}}");
        $this->doInvalidateVersion($modelClass);
    }

    public function flushAll(): int
    {
        return $this->store->flushByPatterns([
            'query:*', 'model:*', 'members:model:*', 'ver:*',
            'agg:*', 'count:*', 'pivot:*', 'through:*', 'scheduled:*', 'building:*',
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

        $nowMs = (int) floor(microtime(true) * 1000);
        $script = <<<'LUA'
            local now = tonumber(ARGV[1])

            local due_at = redis.call('GET', KEYS[2])
            if due_at then
                local due = tonumber(due_at)

                if due and due <= now then
                    redis.call('DEL', KEYS[2])

                    return tostring(redis.call('INCR', KEYS[1]))
                end

                if not due then
                    redis.call('DEL', KEYS[2])
                end
            end

            local ver = redis.call('GET', KEYS[1])

            return ver or '0'
        LUA;

        return $this->store->eval($script, ["ver:{{$classKey}}:", "scheduled:{{$classKey}}:"], [$nowMs]);
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
            $memberKey = $this->store->prefix("members:model:{{$classKey}}");
            $this->store->setManyTracked($attrsByKey, $this->ttl, $memberKey);
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

        return $this->versionLocal[$classKey] = $value !== null ? (int) $value : 0;
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

    private function cacheModelAttrs(string $modelClass, array $modelAttrs): void
    {
        if (empty($modelAttrs)) {
            return;
        }

        $classKey = $this->classKey($modelClass);
        $attrsByKey = [];
        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey["model:{{$classKey}}:{$id}"] = $attrs;
        }

        $memberKey = $this->store->prefix("members:model:{{$classKey}}");
        $this->store->setManyTracked($attrsByKey, $this->ttl, $memberKey);
    }
}
