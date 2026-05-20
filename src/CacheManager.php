<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Support\ModelCacheLoader;
use NormCache\Support\ModelHydrator;

class CacheManager
{
    protected static array $classKeyCache = [];

    protected Connection $connection;

    /** @var array<string, array<string, true>> */
    protected array $flushQueue = [];

    protected bool $igbinary;

    /** @var array<string, int> */
    protected array $versionLocal = [];

    protected ModelCacheLoader $modelCacheLoader;

    public function __construct(
        protected string $redisConnection,
        protected int $ttl,
        protected int $queryTtl,
        protected string $keyPrefix,
        protected int $cooldown,
        protected bool $cluster = false,
        protected bool $enabled = true,
        protected bool $dispatchEvents = true,
        protected bool $fallback = false,
        protected bool $fireRetrieved = false,
    ) {
        $this->igbinary = extension_loaded('igbinary');
        $this->connection = Redis::connection($this->redisConnection);
        $this->modelCacheLoader = new ModelCacheLoader(
            getMany: fn(array $keys) => $this->getMany($keys),
            setManyModels: fn(string $modelClass, array $attrsByKey, int $ttl) => $this->setManyModels($modelClass, $attrsByKey, $ttl),
            classKey: fn(string $modelClass) => $this->classKey($modelClass),
            fireRetrieved: $this->fireRetrieved,
            dispatchEvents: $this->dispatchEvents,
            ttl: $this->ttl,
        );
    }

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
        return $this->fallback;
    }

    public function disable(): void
    {
        $this->enabled = false;
    }

    public function enable(): void
    {
        $this->enabled = true;
    }

    protected function groupByTag(array $keys): array
    {
        if (!$this->cluster) {
            return [$keys];
        }

        $groups = [];
        foreach ($keys as $key) {
            $tag = $key;
            if (preg_match('/\{([^}]+)\}/', $key, $matches)) {
                $tag = $matches[1];
            }
            $groups[$tag][] = $key;
        }

        return $groups;
    }

    public function ttl(): int
    {
        return $this->ttl;
    }

    public function queryTtl(): int
    {
        return $this->queryTtl;
    }

    public function get(string $key): mixed
    {
        $value = $this->connection()->get($this->prefix($key));

        return $value !== null ? $this->unserialize($value) : null;
    }

    public function set(string $key, mixed $value, ?int $ttl = null): mixed
    {
        $ttl = $ttl ?? $this->ttl;

        $this->connection()->setex(
            $this->prefix($key),
            $ttl,
            $this->serialize($value)
        );

        return $value;
    }

    public function setIfAbsent(string $key, mixed $value, int $ttl = 0): bool
    {
        $ttl = $ttl ?: $this->ttl;

        return (bool) $this->connection()->set(
            $this->prefix($key),
            $this->serialize($value),
            'EX',
            $ttl,
            'NX'
        );
    }

    public function delete(string $key): void
    {
        $this->connection()->del($this->prefix($key));
    }

    public function getMany(array $keys): array
    {
        if (empty($keys)) {
            return [];
        }

        if (!$this->cluster) {
            $prefixed = $this->keyPrefix !== '' ? array_map(fn($k) => $this->keyPrefix . $k, $keys) : $keys;

            return array_map(fn($v) => $v !== null ? $this->unserialize($v) : null, $this->connection()->mget($prefixed));
        }

        $groups = $this->groupByTag($keys);
        $results = [];

        foreach ($groups as $groupKeys) {
            $prefixed = array_map(fn($k) => $this->prefix($k), $groupKeys);
            $raw = $this->connection()->mget($prefixed);

            $idx = 0;
            foreach ($groupKeys as $key) {
                $value = $raw[$idx++];
                $results[$key] = $value !== null ? $this->unserialize($value) : null;
            }
        }

        return array_map(fn($k) => $results[$k], $keys);
    }

    public function setMany(array $pairs, int $ttl): void
    {
        if (empty($pairs)) {
            return;
        }

        $groups = $this->groupByTag(array_keys($pairs));
        $connection = $this->connection();

        foreach ($groups as $keys) {
            $connection->pipeline(function ($pipe) use ($keys, $pairs, $ttl) {
                foreach ($keys as $key) {
                    $pipe->setex($this->prefix($key), $ttl, $this->serialize($pairs[$key]));
                }
            });
        }
    }

    public function setManyModels(string $modelClass, array $attrsByKey, int $ttl): void
    {
        if (empty($attrsByKey)) {
            return;
        }

        $classKey = $this->classKey($modelClass);
        $memberKey = $this->prefix("members:model:{{$classKey}}");
        $groups = $this->groupByTag(array_keys($attrsByKey));
        $connection = $this->connection();

        foreach ($groups as $keys) {
            $connection->pipeline(function ($pipe) use ($keys, $attrsByKey, $ttl, $memberKey) {
                $prefixedKeys = [];
                foreach ($keys as $key) {
                    $p = $this->prefix($key);
                    $prefixedKeys[] = $p;
                    $pipe->setex($p, $ttl, $this->serialize($attrsByKey[$key]));
                }
                $pipe->sadd($memberKey, ...$prefixedKeys);
                $pipe->expire($memberKey, $ttl);
            });
        }
    }

    public function setAndReleaseLock(string $key, mixed $value, int $ttl, string $lockKey): void
    {
        $keys = [$key, $lockKey];
        $groups = $this->groupByTag($keys);

        if (count($groups) !== 1) {
            $this->set($key, $value, $ttl);
            $this->delete($lockKey);

            return;
        }

        $this->connection()->pipeline(function ($pipe) use ($key, $value, $ttl, $lockKey) {
            $pipe->setex($this->prefix($key), $ttl, $this->serialize($value));
            $pipe->del($this->prefix($lockKey));
        });
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash): array
    {
        $classKey = $this->classKey($modelClass);
        $version = $this->currentVersion($modelClass);
        $key = "{$namespace}:{{$classKey}}:v{$version}:{$hash}";

        return [
            'key' => $key,
            'data' => $this->get($key),
            'version' => $version,
        ];
    }

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
                local lock_key = KEYS[4] .. ver .. ':' .. ARGV[1]
                local acquired = redis.call('SET', lock_key, 1, 'EX', 5, 'NX')
                return {'miss', ver, acquired and 1 or 0}
            end

            local ok, ids = pcall(cjson.decode, ids_raw)
            if not ok or type(ids) ~= 'table' then
                redis.call('DEL', query_key)
                local lock_key = KEYS[4] .. ver .. ':' .. ARGV[1]
                local acquired = redis.call('SET', lock_key, 1, 'EX', 5, 'NX')
                return {'corrupt', ver, acquired and 1 or 0}
            end

            if #ids == 0 then
                return {'empty', ver}
            end

            local model_keys = {}
            for i, id in ipairs(ids) do
                model_keys[i] = KEYS[5] .. id
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

        $result = $this->connection()->eval(
            $script,
            5,
            $this->prefix("ver:{{$classKey}}:"),
            $this->prefix("scheduled:{{$classKey}}:"),
            $this->prefix("query:{{$classKey}}:v"),
            $this->prefix("building:query:{{$classKey}}:v"),
            $this->prefix("model:{{$classKey}}:"),
            $hash,
            (int) floor(microtime(true) * 1000)
        );

        if (!is_array($result)) {
            $version = $this->currentVersion($modelClass);

            return ['key' => "query:{{$classKey}}:v{$version}:{$hash}", 'ids' => null, 'models' => null, 'lock' => null];
        }

        $status = $result[0] ?? null;
        $version = (int) ($result[1] ?? 0);
        $this->versionLocal[$classKey] = $version;
        $queryKey = "query:{{$classKey}}:v{$version}:{$hash}";

        if ($status === 'miss') {
            $lock = (int) ($result[2] ?? 0) === 1 ? "building:{$queryKey}" : null;

            return ['key' => $queryKey, 'ids' => null, 'models' => null, 'lock' => $lock];
        }

        if ($status === 'corrupt') {
            return ['key' => $queryKey, 'ids' => null, 'models' => null, 'lock' => "building:{$queryKey}"];
        }

        if ($status === 'empty') {
            return ['key' => $queryKey, 'ids' => [], 'models' => [], 'lock' => null];
        }

        if ($status !== 'hit') {
            return ['key' => $queryKey, 'ids' => null, 'models' => null, 'lock' => null];
        }

        $models = array_map(
            fn($value) => $value !== null && $value !== false ? $this->unserialize($value) : null,
            $result[3]
        );

        return ['key' => $queryKey, 'ids' => $result[2], 'models' => $models, 'lock' => null];
    }

    public function getQueryIds(string $key): ?array
    {
        $value = $this->connection()->get($this->prefix($key));
        if ($value === null) {
            return null;
        }
        $decoded = json_decode($value, true);

        return is_array($decoded) ? $decoded : null;
    }

    public function poll(callable $getter): mixed
    {
        $delay = 20_000;
        for ($i = 0; $i < 5; $i++) {
            usleep($delay);
            $result = $getter();
            if ($result !== null) {
                return $result;
            }
            $delay = min($delay * 2, 200_000);
        }

        return null;
    }

    /**
     * Runs a write-side cache operation. No-ops when disabled. On error applies the
     * fallback policy (re-throw or report+disable). Read paths use triggerFallback()
     * directly inside their own catch blocks so they can run their own recovery action.
     */
    public function handle(callable $operation): void
    {
        if (!$this->enabled) {
            return;
        }

        try {
            $operation();
        } catch (\Exception $e) {
            $this->triggerFallback($e);
        }
    }

    /**
     * Applies the fallback policy for read-path catch blocks. Re-throws when fallback
     * is disabled so callers see Redis errors directly; otherwise reports and disables
     * the cache so the caller can proceed with its DB recovery action.
     */
    public function triggerFallback(\Exception $e): void
    {
        if (!$this->fallback) {
            throw $e;
        }

        report($e);
        $this->disable();
    }

    public function setQueryResults(string $key, array $ids, int $ttl): void
    {
        $this->connection()->setex(
            $this->prefix($key),
            $ttl,
            json_encode($ids)
        );
    }

    public function setQueryResultsAndReleaseLock(string $key, array $ids, int $ttl, string $lockKey): void
    {
        $keys = [$key, $lockKey];
        $groups = $this->groupByTag($keys);
        $json = json_encode($ids);

        if (count($groups) !== 1) {
            $this->setQueryResults($key, $ids, $ttl);
            $this->delete($lockKey);

            return;
        }

        $this->connection()->pipeline(function ($pipe) use ($key, $json, $ttl, $lockKey) {
            $pipe->setex($this->prefix($key), $ttl, $json);
            $pipe->del($this->prefix($lockKey));
        });
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->classKey($relatedClass);
        $throughKey = $this->classKey($throughClass);
        $throughVersion = $this->currentVersion($throughClass);
        $relatedVersion = $this->currentVersion($relatedClass);
        $key = "through:{{$relatedKey}}:{$throughKey}:v{$relatedVersion}:v{$throughVersion}:{$hash}";
        $data = $this->get($key);

        if ($data !== null) {
            return ['key' => $key, 'data' => $data, 'lock' => null];
        }

        $lockKey = $this->setIfAbsent("building:{$key}", 1, 5) ? "building:{$key}" : null;

        return ['key' => $key, 'data' => null, 'lock' => $lockKey];
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

    public function getModels(
        array $ids,
        string $modelClass,
        ?array $columns = null,
        ?array $raw = null,
        ?EloquentBuilder $missedQuery = null,
        bool $preserveQueryShape = true,
    ): array {
        return $this->modelCacheLoader->getModels(
            $ids,
            $modelClass,
            $columns,
            $raw,
            $missedQuery,
            $preserveQueryShape,
        );
    }

    public function currentVersion(string $modelClass): int
    {
        $classKey = $this->classKey($modelClass);

        if (isset($this->versionLocal[$classKey])) {
            return $this->versionLocal[$classKey];
        }

        $value = $this->resolveCurrentVersion($classKey);

        return $this->versionLocal[$classKey] = $value !== null ? (int) $value : 0;
    }

    public function flushVersionLocal(): void
    {
        $this->versionLocal = [];
    }

    private function doInvalidateVersion(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        if ($this->cooldown <= 0) {
            $this->versionLocal[$classKey] = (int) $this->connection()->incr(
                $this->prefix("ver:{{$classKey}}:")
            );

            return;
        }

        $this->resolveCurrentVersion($classKey);
        $this->scheduleInvalidation($classKey);
    }

    private function resolveCurrentVersion(string $classKey): string|int|null
    {
        $versionKey = $this->prefix("ver:{{$classKey}}:");

        if ($this->cooldown <= 0) {
            return $this->connection()->get($versionKey);
        }

        $scheduledKey = $this->prefix("scheduled:{{$classKey}}:");
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

        return $this->connection()->eval($script, 2, $versionKey, $scheduledKey, $nowMs);
    }

    private function scheduleInvalidation(string $classKey): void
    {
        $dueAtMs = (int) floor(microtime(true) * 1000) + ($this->cooldown * 1000);

        $this->connection()->setnx(
            $this->prefix("scheduled:{{$classKey}}:"),
            (string) $dueAtMs
        );
    }

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

    public function flushClass(Model $model): void
    {
        if (!$this->enabled) {
            return;
        }

        $conn = $model->getConnection()->getName();

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueModelFlush($conn, $model::class);

            return;
        }

        $this->handle(fn() => $this->flushModel($model::class));
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
                $this->flushModel($modelClass);
            }
        });
    }

    public function discardPending(string $connectionName): void
    {
        unset($this->flushQueue[$connectionName]);
    }

    public function classKey(string $class): string
    {
        return self::$classKeyCache[$class] ??= $this->resolveClassKey($class);
    }

    private function resolveClassKey(string $class): string
    {
        $model = ModelHydrator::prototype($class);
        $connection = $model->getConnectionName() ?? DB::getDefaultConnection();

        return "{$connection}:{$model->getTable()}";
    }

    public function modelKey(string $modelClass, string $id): string
    {
        return 'model:{' . $this->classKey($modelClass) . '}:' . $id;
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
            $prefixedKey = $this->prefix($key);
            $memberKey = $this->prefix("members:model:{{$classKey}}");
            $this->connection()->pipeline(function ($pipe) use ($prefixedKey, $memberKey) {
                $pipe->del($prefixedKey);
                $pipe->srem($memberKey, $prefixedKey);
            });
            $this->doInvalidateVersion($class);
        });
    }

    public function flushModel(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);
        $memberKey = $this->prefix("members:model:{{$classKey}}");
        $keys = $this->connection()->smembers($memberKey);

        if (!empty($keys)) {
            $this->asyncDel($keys);
        }

        $this->connection()->del($memberKey);
        $this->doInvalidateVersion($modelClass);
    }

    public function getFlushPatterns(): array
    {
        return [
            'query:*',
            'model:*',
            'members:model:*',
            'ver:*',
            'agg:*',
            'count:*',
            'pivot:*',
            'through:*',
            'scheduled:*',
            'building:*',
        ];
    }

    public function flushAll(): int
    {
        $total = 0;
        $connection = $this->connection();

        foreach ($this->getFlushPatterns() as $pattern) {
            $keys = $connection->keys($this->prefix($pattern));
            if (!empty($keys)) {
                $total += count($keys);
                $this->asyncDel($keys);
            }
        }

        return $total;
    }

    protected function connection(): Connection
    {
        return $this->connection;
    }

    protected function asyncDel(array $prefixedKeys): void
    {
        if (empty($prefixedKeys)) {
            return;
        }

        $groups = $this->groupByTag($prefixedKeys);
        $connection = $this->connection();

        foreach ($groups as $keys) {
            foreach (array_chunk($keys, 1000) as $chunk) {
                $connection->unlink(...$chunk);
            }
        }
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc'): array
    {
        $parentKey = $this->classKey($parentClass);
        $relatedKey = $this->classKey($relatedClass);

        $parentVersion = $this->currentVersion($parentClass);
        $relatedVersion = $this->currentVersion($relatedClass);

        $prefix = "pivot:{{$parentKey}}:{$relatedKey}:{$relation}:{$constraintHash}:v{$parentVersion}:v{$relatedVersion}:";

        $keys = array_map(fn($id) => $prefix . $id, $parentIds);
        $data = $this->getMany($keys);

        return [
            'parentVersion' => $parentVersion,
            'relatedVersion' => $relatedVersion,
            'data' => array_combine($parentIds, $data),
        ];
    }

    protected function prefix(string $key): string
    {
        return $this->keyPrefix !== '' ? $this->keyPrefix . $key : $key;
    }

    private function queueModelFlush(string $connectionName, string $modelClass): void
    {
        $this->flushQueue[$connectionName][$modelClass] = true;
    }

    protected function serialize(mixed $value): mixed
    {
        if (is_numeric($value) && is_finite($value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_serialize($value) : serialize($value);
    }

    protected function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_unserialize($value) : unserialize($value);
    }
}
