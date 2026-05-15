<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;

class CacheManager
{
    protected static array $classKeyCache = [];

    protected static array $modelPrototypes = [];

    protected static array $modelHydrators = [];

    protected static array $deletedAtColumns = [];

    protected ?Connection $connection = null;

    /** @var array<string, array<string, true>> */
    protected array $invalidationQueue = [];

    /** @var array<string, array<string, true>> */
    protected array $deleteQueue = [];

    /** @var array<string, array<string, true>> */
    protected array $flushQueue = [];

    /** @var array<string, int> */
    protected array $versionLocal = [];

    protected bool $igbinary;

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
        $this->igbinary = \extension_loaded('igbinary');
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

        return ($value !== null && $value !== false) ? $this->unserialize($value) : null;
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

            return array_map(fn($v) => ($v !== null && $v !== false) ? $this->unserialize($v) : null, $this->connection()->mget($prefixed));
        }

        $groups = $this->groupByTag($keys);
        $results = [];

        foreach ($groups as $groupKeys) {
            $prefixed = array_map(fn($k) => $this->prefix($k), $groupKeys);
            $raw = $this->connection()->mget($prefixed);

            $idx = 0;
            foreach ($groupKeys as $key) {
                $v = $raw[$idx++];
                $results[$key] = ($v !== null && $v !== false) ? $this->unserialize($v) : null;
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
            });
        }
    }

    public function setAndReleaseLock(string $key, mixed $value, int $ttl, string $lockKey): void
    {
        $keys = [$key, $lockKey];
        $groups = $this->groupByTag($keys);

        if (count($groups) === 1) {
            $this->connection()->pipeline(function ($pipe) use ($key, $value, $ttl, $lockKey) {
                $pipe->setex($this->prefix($key), $ttl, $this->serialize($value));
                $pipe->del($this->prefix($lockKey));
            });
        } else {
            $this->set($key, $value, $ttl);
            $this->delete($lockKey);
        }
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash): array
    {
        $classKey = $this->classKey($modelClass);

        $script = <<<'LUA'
            if ARGV[2] == '1' and redis.call('GET', KEYS[3]) and not redis.call('GET', KEYS[4]) then
                redis.call('DEL', KEYS[3])
                redis.call('INCR', KEYS[1])
            end
            local ver = redis.call('GET', KEYS[1])
            if not ver then ver = '0' end
            local data = redis.call('GET', KEYS[2] .. ver .. ':' .. ARGV[1])
            if data then return {ver, data} end
            return {ver}
        LUA;

        $result = $this->connection()->eval(
            $script,
            4,
            $this->prefix("ver:{{$classKey}}:"),
            $this->prefix("{$namespace}:{{$classKey}}:v"),
            $this->prefix("pending:{{$classKey}}:"),
            $this->prefix("cooldown:{{$classKey}}:"),
            $hash,
            $this->cooldown > 0 ? '1' : '0'
        );

        if (!is_array($result)) {
            $version = $this->currentVersion($modelClass);

            return ['key' => "{$namespace}:{{$classKey}}:v{$version}:{$hash}", 'data' => null, 'version' => $version];
        }

        $version = (int) $result[0];
        $this->versionLocal[$classKey] = $version;
        $key = "{$namespace}:{{$classKey}}:v{$version}:{$hash}";

        return [
            'key' => $key,
            'data' => count($result) > 1 ? $this->unserialize($result[1]) : null,
            'version' => $version,
        ];
    }

    public function getModelsFromQuery(string $modelClass, string $hash): array
    {
        $classKey = $this->classKey($modelClass);

        // Single EVAL: version + query + lock (miss) or version + query + model MGET (hit).
        // Miss: {ver, 1} = lock acquired, {ver, 0} = contended. Hit: {ver, ids, models}.
        $script = <<<'LUA'
            if ARGV[2] == '1' and redis.call('GET', KEYS[5]) and not redis.call('GET', KEYS[6]) then
                redis.call('DEL', KEYS[5])
                redis.call('INCR', KEYS[1])
            end
            local ver = redis.call('GET', KEYS[1])
            if not ver then ver = '0' end
            local ids_raw = redis.call('GET', KEYS[3] .. ver .. ':' .. ARGV[1])
            if not ids_raw then
                local acquired = redis.call('SET', KEYS[4] .. ver .. ':' .. ARGV[1], 1, 'EX', 5, 'NX') -- 5s TTL: auto-expires if builder dies
                return {ver, acquired and 1 or 0}
            end
            local ids = cjson.decode(ids_raw)
            if #ids == 0 then return {ver, {}, {}} end
            local model_keys = {}
            for _, id in ipairs(ids) do
                table.insert(model_keys, KEYS[2] .. id)
            end
            return {ver, ids, redis.call('MGET', unpack(model_keys))}
        LUA;

        $result = $this->connection()->eval(
            $script,
            6,
            $this->prefix("ver:{{$classKey}}:"),
            $this->prefix("model:{{$classKey}}:"),
            $this->prefix("query:{{$classKey}}:v"),
            $this->prefix("building:query:{{$classKey}}:v"),
            $this->prefix("pending:{{$classKey}}:"),
            $this->prefix("cooldown:{{$classKey}}:"),
            $hash,
            $this->cooldown > 0 ? '1' : '0'
        );

        if (!is_array($result)) {
            $version = $this->currentVersion($modelClass);

            return ['key' => "query:{{$classKey}}:v{$version}:{$hash}", 'ids' => null, 'models' => null, 'lock' => null];
        }

        $this->versionLocal[$classKey] = (int) $result[0];
        $queryKey = "query:{{$classKey}}:v{$result[0]}:{$hash}";

        // Miss: {ver, 1|0} — 1 = lock acquired, 0 = contended
        if (count($result) === 2) {
            $lock = (int) $result[1] === 1 ? "building:{$queryKey}" : null;

            return ['key' => $queryKey, 'ids' => null, 'models' => null, 'lock' => $lock];
        }

        return [
            'key' => $queryKey,
            'ids' => $result[1],
            'models' => $result[2],
            'lock' => null,
        ];
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

        if (count($groups) === 1) {
            $this->connection()->pipeline(function ($pipe) use ($key, $json, $ttl, $lockKey) {
                $pipe->setex($this->prefix($key), $ttl, $json);
                $pipe->del($this->prefix($lockKey));
            });
        } else {
            $this->setQueryResults($key, $ids, $ttl);
            $this->delete($lockKey);
        }
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->classKey($relatedClass);
        $throughKey = $this->classKey($throughClass);

        // throughVersion is fetched separately: in cluster mode ver:{throughKey}: lives on a
        // different slot than the {relatedKey}-tagged through/lock keys, so it can't share an EVAL.
        // Single EVAL: ver:{relatedKey} + through-key (hit) or lock NX (miss).
        // Hit: {ver, 1, data}. Miss: {ver, 0, 1} = lock acquired, {ver, 0, 0} = contended.
        $throughVersion = $this->currentVersion($throughClass);

        $script = <<<'LUA'
            if ARGV[3] == '1' and redis.call('GET', KEYS[4]) and not redis.call('GET', KEYS[5]) then
                redis.call('DEL', KEYS[4])
                redis.call('INCR', KEYS[1])
            end
            local ver1 = redis.call('GET', KEYS[1])
            if not ver1 then ver1 = '0' end
            local suffix = ver1 .. ':v' .. ARGV[2] .. ':' .. ARGV[1]
            local data = redis.call('GET', KEYS[2] .. suffix)
            if data then return {ver1, 1, data} end
            local acquired = redis.call('SET', KEYS[3] .. suffix, 1, 'EX', 5, 'NX')
            return {ver1, 0, acquired and 1 or 0}
        LUA;

        $result = $this->connection()->eval(
            $script,
            5,
            $this->prefix("ver:{{$relatedKey}}:"),
            $this->prefix("through:{{$relatedKey}}:{$throughKey}:v"),
            $this->prefix("building:through:{{$relatedKey}}:{$throughKey}:v"),
            $this->prefix("pending:{{$relatedKey}}:"),
            $this->prefix("cooldown:{{$relatedKey}}:"),
            $hash,
            $throughVersion,
            $this->cooldown > 0 ? '1' : '0'
        );

        if (!is_array($result)) {
            $relatedVersion = $this->currentVersion($relatedClass);

            return [
                'key' => "through:{{$relatedKey}}:{$throughKey}:v{$relatedVersion}:v{$throughVersion}:{$hash}",
                'data' => null,
                'lock' => null,
            ];
        }

        $relatedVersion = (int) $result[0];
        $this->versionLocal[$relatedKey] = $relatedVersion;
        $key = "through:{{$relatedKey}}:{$throughKey}:v{$relatedVersion}:v{$throughVersion}:{$hash}";

        if ((int) $result[1] === 1) {
            return ['key' => $key, 'data' => $this->unserialize($result[2]), 'lock' => null];
        }

        $lockKey = (int) $result[2] === 1 ? "building:{$key}" : null;

        return ['key' => $key, 'data' => null, 'lock' => $lockKey];
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connectionName = null): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            foreach ($modelClasses as $modelClass) {
                $this->invalidationQueue[$connectionName][$modelClass] = true;
            }

            return;
        }

        foreach ($modelClasses as $modelClass) {
            $this->doInvalidateVersion($modelClass);
        }
    }

    public function getModels(array $ids, string $modelClass, ?array $columns = null, ?array $raw = null, ?EloquentBuilder $missedQuery = null): array
    {
        if ($ids === []) {
            return [];
        }

        $classKey = $this->classKey($modelClass);
        if ($raw === null) {
            $keys = array_map(fn($id) => "model:{{$classKey}}:" . $id, $ids);
            $raw = $this->getMany($keys);
        }

        $prototype = self::$modelPrototypes[$modelClass] ??= new $modelClass;
        $projection = $this->normalizeColumns($columns);

        $result = [];
        $missed = [];

        $hydrator = self::$modelHydrators[$modelClass] ??= \Closure::bind(static function ($model, $attributes, $fire) {
            $model->attributes = $attributes;
            $model->original = $attributes;
            $model->exists = true;
            $model->classCastCache = [];
            $model->attributeCastCache = [];

            if ($fire) {
                $model->fireModelEvent('retrieved', false);
            }
        }, null, $modelClass);

        foreach ($ids as $i => $id) {
            $attrs = $raw[$i];

            if ($attrs === null || $attrs === false) {
                $missed[] = $id;

                continue;
            }

            if (is_string($attrs)) {
                $attrs = $this->unserialize($attrs);
            }

            if (!is_array($attrs)) {
                $missed[] = $id;

                continue;
            }

            if ($projection !== null) {
                $attrs = $this->projectAttributes($attrs, $projection);
            }

            $instance = clone $prototype;
            $hydrator($instance, $attrs, $this->fireRetrieved);

            $result[$id] = $instance;
        }

        if ($missed === []) {
            if ($this->dispatchEvents && $result !== []) {
                event(new ModelCacheHit($modelClass, array_keys($result)));
            }

            return array_values($result);
        }

        if ($this->dispatchEvents) {
            if ($result !== []) {
                event(new ModelCacheHit($modelClass, array_keys($result)));
            }
            event(new ModelCacheMiss($modelClass, $missed));
        }

        $fetched = $this->fetchAndCacheFromDatabase($missed, $modelClass, $classKey, $projection, $missedQuery);

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($result[$id]) || isset($fetched[$id])) {
                $ordered[] = $result[$id] ?? $fetched[$id];
            }
        }

        return $ordered;
    }

    /**
     * @return array<string, string>|null output attribute => source attribute
     */
    private function normalizeColumns(?array $columns): ?array
    {
        if ($columns === null) {
            return null;
        }

        $normalized = [];
        foreach ($columns as $col) {
            $col = (string) $col;
            [$source, $output] = $this->normalizeColumnProjection($col);
            $normalized[$output] = $source;
        }

        return $normalized;
    }

    private function normalizeColumnProjection(string $column): array
    {
        $column = trim($column);
        $segments = preg_split('/\s+as\s+/i', $column);

        if (count($segments) === 2) {
            return [
                $this->unqualifyColumn($segments[0]),
                $this->unqualifyColumn($segments[1]),
            ];
        }

        $name = $this->unqualifyColumn($column);

        return [$name, $name];
    }

    private function unqualifyColumn(string $column): string
    {
        $column = trim($column);
        $dotPos = strrpos($column, '.');

        if ($dotPos !== false) {
            $column = substr($column, $dotPos + 1);
        }

        return trim($column, " \t\n\r\0\x0B`\"[]");
    }

    private function projectAttributes(array $attributes, array $projection): array
    {
        $projected = [];

        foreach ($projection as $output => $source) {
            if (array_key_exists($source, $attributes)) {
                $projected[$output] = $attributes[$source];
            }
        }

        return $projected;
    }

    private function fetchAndCacheFromDatabase(array $missed, string $modelClass, string $classKey, ?array $projection, ?EloquentBuilder $missedQuery = null): array
    {
        $prototype = self::$modelPrototypes[$modelClass];
        $pk = $prototype->getKeyName();
        $query = $this->prepareMissedQuery($modelClass, $missedQuery);
        $loaded = $query->whereKey($missed)
            ->get(['*'])
            ->keyBy($pk);

        $inserts = [];
        $deletedAtCol = self::$deletedAtColumns[$modelClass] ??= method_exists($prototype, 'getDeletedAtColumn')
            ? $prototype->getDeletedAtColumn()
            : null;

        foreach ($loaded as $id => $model) {
            $attrs = $model->getRawOriginal();

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if (!$isTrashed) {
                $inserts["model:{{$classKey}}:$id"] = $this->serialize($attrs);
            }

            if ($projection !== null) {
                $model->setRawAttributes($this->projectAttributes($attrs, $projection), true);
            }
        }

        if ($inserts !== []) {
            $memberKey = $this->prefix("members:model:{{$classKey}}");
            $this->connection()->pipeline(function ($pipe) use ($inserts, $memberKey) {
                $prefixedKeys = [];
                foreach ($inserts as $key => $value) {
                    $p = $this->prefix($key);
                    $prefixedKeys[] = $p;
                    $pipe->setex($p, $this->ttl, $value);
                }
                $pipe->sadd($memberKey, ...$prefixedKeys);
            });
        }

        return $loaded->all();
    }

    private function prepareMissedQuery(string $modelClass, ?CacheableBuilder $missedQuery): EloquentBuilder
    {
        if ($missedQuery === null) {
            return $modelClass::query()->withoutCache();
        }

        $query = clone $missedQuery;
        $query->withoutCache();
        $query->setEagerLoads([]);
        $query->setQuery(
            $query->getQuery()
                ->cloneWithout(['columns', 'orders', 'limit', 'offset'])
                ->cloneWithoutBindings(['select', 'order'])
        );

        return $query;
    }

    public function currentVersion(string $modelClass): int
    {
        $classKey = $this->classKey($modelClass);

        if ($this->cooldown > 0) {
            $script = <<<'LUA'
                if redis.call('GET', KEYS[2]) and not redis.call('GET', KEYS[3]) then
                    redis.call('DEL', KEYS[2])
                    redis.call('INCR', KEYS[1])
                end
                return redis.call('GET', KEYS[1])
            LUA;
            $value = $this->connection()->eval(
                $script,
                3,
                $this->prefix("ver:{{$classKey}}:"),
                $this->prefix("pending:{{$classKey}}:"),
                $this->prefix("cooldown:{{$classKey}}:")
            );
        } else {
            $value = $this->connection()->get($this->prefix("ver:{{$classKey}}:"));
        }

        return $this->versionLocal[$classKey] = $value !== null ? (int) $value : 0;
    }

    public function flushVersionLocal(): void
    {
        $this->versionLocal = [];
    }

    public function invalidateVersion(Model $model): void
    {
        $conn = $model->getConnectionName();

        if ($conn !== null && DB::connection($conn)->transactionLevel() > 0) {
            $this->invalidationQueue[$conn][$model::class] = true;

            return;
        }

        $this->doInvalidateVersion($model::class);
    }

    public function deferDelete(string $key, ?string $connectionName = null): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            $this->deleteQueue[$connectionName][$key] = true;

            return;
        }

        $this->connection()->del($this->prefix($key));
    }

    public function deferFlushModel(Model $model): void
    {
        $conn = $model->getConnectionName();

        if ($conn !== null && DB::connection($conn)->transactionLevel() > 0) {
            $this->flushQueue[$conn][$model::class] = true;

            return;
        }

        $this->flushModel($model::class);
    }

    public function commitPending(string $connectionName): void
    {
        $invalidations = array_keys($this->invalidationQueue[$connectionName] ?? []);
        $deletes = array_keys($this->deleteQueue[$connectionName] ?? []);
        $flushes = array_keys($this->flushQueue[$connectionName] ?? []);

        unset(
            $this->invalidationQueue[$connectionName],
            $this->deleteQueue[$connectionName],
            $this->flushQueue[$connectionName]
        );

        if (empty($invalidations) && empty($deletes) && empty($flushes)) {
            return;
        }

        if (!empty($deletes)) {
            $this->asyncDel(array_map(fn($k) => $this->prefix($k), $deletes));

            $byClass = [];
            foreach ($deletes as $k) {
                if (preg_match('/\{([^}]+)\}/', $k, $m)) {
                    $byClass[$m[1]][] = $this->prefix($k);
                }
            }
            foreach ($byClass as $classKey => $prefixedKeys) {
                $this->connection()->srem($this->prefix("members:model:{{$classKey}}"), ...$prefixedKeys);
            }
        }

        foreach ($flushes as $modelClass) {
            $this->flushModel($modelClass);
        }

        foreach ($invalidations as $modelClass) {
            $this->doInvalidateVersion($modelClass);
        }
    }

    public function discardPending(string $connectionName): void
    {
        unset(
            $this->invalidationQueue[$connectionName],
            $this->deleteQueue[$connectionName],
            $this->flushQueue[$connectionName]
        );
    }

    protected function doInvalidateVersion(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        if ($this->cooldown > 0) {
            $cooldownKey = $this->prefix("cooldown:{{$classKey}}:");
            $verKey = $this->prefix("ver:{{$classKey}}:");
            $pendingKey = $this->prefix("pending:{{$classKey}}:");

            $script = <<<'LUA'
                if redis.call('SET', KEYS[1], 1, 'EX', ARGV[1], 'NX') then
                    redis.call('DEL', KEYS[3])
                    return redis.call('INCR', KEYS[2])
                end
                redis.call('SET', KEYS[3], 1)
                return nil
            LUA;
            $newVer = $this->connection()->eval($script, 3, $cooldownKey, $verKey, $pendingKey, $this->cooldown);

            if (is_numeric($newVer)) {
                $this->versionLocal[$classKey] = (int) $newVer;
            }

            return;
        }

        $this->versionLocal[$classKey] = (int) $this->connection()->incr(
            $this->prefix("ver:{{$classKey}}:")
        );
    }

    public function classKey(string $class): string
    {
        $model = self::$modelPrototypes[$class] ??= new $class;

        if ($model->getConnectionName() === null) {
            return $this->resolveRuntimeClassKey($model);
        }

        return self::$classKeyCache[$class] ??= $this->resolveClassKey($class);
    }

    private function resolveClassKey(string $class): string
    {
        $model = self::$modelPrototypes[$class] ??= new $class;
        $connection = $model->getConnectionName();

        return $connection === null
            ? $model->getTable()
            : "{$connection}:{$model->getTable()}";
    }

    private function resolveRuntimeClassKey(Model $model): string
    {
        return DB::getDefaultConnection() . ':' . $model->getTable();
    }

    public function modelKey(string $modelClass, string $id): string
    {
        return 'model:{' . $this->classKey($modelClass) . '}:' . $id;
    }

    public function flushInstance(Model $model): void
    {
        $conn = $model->getConnectionName();
        $class = $model::class;
        $key = $this->modelKey($class, $model->getKey());

        if ($conn !== null && DB::connection($conn)->transactionLevel() > 0) {
            $this->deleteQueue[$conn][$key] = true;
            $this->invalidationQueue[$conn][$class] = true;

            return;
        }

        $classKey = $this->classKey($class);
        $prefixedKey = $this->prefix($key);
        $memberKey = $this->prefix("members:model:{{$classKey}}");
        $this->connection()->pipeline(function ($pipe) use ($prefixedKey, $memberKey) {
            $pipe->del($prefixedKey);
            $pipe->srem($memberKey, $prefixedKey);
        });
        $this->doInvalidateVersion($class);
    }

    public function flushModel(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        $this->doInvalidateVersion($modelClass);

        $memberKey = $this->prefix("members:model:{{$classKey}}");
        $keys = $this->connection()->smembers($memberKey);

        if (!empty($keys)) {
            $this->asyncDel($keys);
        }

        $this->connection()->del($memberKey);
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
            'cooldown:*',
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
        return $this->connection ??= Redis::connection($this->redisConnection);
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
