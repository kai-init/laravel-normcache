<?php

namespace NormCache;

use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;

class CacheManager
{
    protected static array $classKeyCache = [];

    protected ?Connection $connection = null;

    /** @var array<string, array<string, true>> */
    protected array $invalidationQueue = [];

    /** @var array<string, array<string, true>> */
    protected array $deleteQueue = [];

    /** @var array<string, array<string, true>> */
    protected array $flushQueue = [];

    /** @var array<string, int> L1 in-process version cache, keyed by classKey */
    protected array $versionLocal = [];

    protected bool $igbinary;

    public function __construct(
        protected string $redisConnection,
        protected int $ttl,
        protected int $queryTtl,
        protected string $keyPrefix,
        protected int $cooldown,
        protected bool $cluster = false,
    ) {
        $this->igbinary = \extension_loaded('igbinary');
    }

    public function isEnabled(): bool
    {
        return config('normcache.enabled', true);
    }

    protected function groupByTag(array $keys): array
    {
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

        $prefixed = array_map(fn($k) => $this->prefix($k), $keys);
        $raw = $this->connection()->mget($prefixed);

        return array_map(
            fn($v) => ($v !== null && $v !== false) ? $this->unserialize($v) : null,
            $raw
        );
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
            // Keys are in different slots, cannot pipeline atomically
            $this->set($key, $value, $ttl);
            $this->delete($lockKey);
        }
    }

    public function getNamespacedCache(string $namespace, string $modelClass, string $hash): array
    {
        $classKey = $this->classKey($modelClass);
        $version = $this->currentVersion($modelClass);
        
        $key = "{$namespace}:{{$classKey}}:v{$version}:{$hash}";
        $data = $this->get($key);

        return [
            'key' => $key,
            'data' => $data,
            'version' => $version,
        ];
    }

    public function getThroughCache(string $relatedClass, string $throughClass, string $hash): array
    {
        $relatedKey = $this->classKey($relatedClass);
        $throughKey = $this->classKey($throughClass);

        $relatedVersion = $this->currentVersion($relatedClass);
        $throughVersion = $this->currentVersion($throughClass);

        $key = "through:{{$relatedKey}}:{$throughKey}:v{$relatedVersion}:v{$throughVersion}:{$hash}";
        $data = $this->get($key);

        return [
            'key' => $key,
            'data' => $data,
        ];
    }

    public function getAggregateCache(string $modelClass, string $versionClass, array $ids, string $column, string $function, string $relation, string $hash): array
    {
        $classKey = $this->classKey($modelClass);
        $version = $this->currentVersion($versionClass);
        
        $aggPrefix = "agg:{{$classKey}}:";
        $aggSuffix = ":{$column}:{$function}:{$relation}:{$hash}:v{$version}";

        $keys = array_map(fn($id) => "{$aggPrefix}{$id}{$aggSuffix}", $ids);
        $data = $this->getMany($keys);

        return [
            'version' => $version,
            'data' => array_combine($ids, $data),
        ];
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

    public function getModels(array $ids, string $modelClass, ?array $columns = null): array
    {
        if ($ids === []) {
            return [];
        }

        $classKey = $this->classKey($modelClass);
        $prefix = $this->keyPrefix . "model:{{$classKey}}:";

        $keys = array_map(fn($id) => $prefix . $id, $ids);

        $raw = $this->connection()->mget($keys);
        $cached = array_combine($ids, $raw);

        $missed = [];
        $result = [];
        $prototype = new $modelClass;

        $normalizedCols = null;
        if ($columns !== null) {
            $normalizedCols = [];
            foreach ($columns as $col) {
                $col = (string) $col;
                $dotPos = strrpos($col, '.');
                $normalizedCols[$dotPos === false ? $col : substr($col, $dotPos + 1)] = true;
            }
        }

        foreach ($cached as $id => $item) {
            if (!$item) {
                $missed[] = $id;
                continue;
            }

            $attrs = $this->unserialize($item);

            if (!is_array($attrs)) {
                $missed[] = $id;
                continue;
            }

            if ($normalizedCols !== null) {
                $attrs = array_intersect_key($attrs, $normalizedCols);
            }

            $instance = clone $prototype;
            $instance->exists = true;
            $instance->setRawAttributes($attrs, true);

            $result[$id] = $instance;
        }

        $hitIds = array_diff($ids, $missed);
        if ($hitIds !== []) {
            event(new ModelCacheHit($modelClass, array_values($hitIds)));
        }
        if ($missed !== []) {
            event(new ModelCacheMiss($modelClass, $missed));
        }

        if ($missed !== []) {
            $pk = $prototype->getKeyName();
            $loaded = $modelClass::query()
                ->withoutCache()
                ->withoutGlobalScope(SoftDeletingScope::class)
                ->findMany($missed)
                ->keyBy($pk);

            $inserts = [];

            foreach ($loaded as $id => $model) {
                $inserts[$prefix . $id] = $this->serialize($model->getAttributes());

                if ($normalizedCols !== null) {
                    $model->setRawAttributes(
                        array_intersect_key($model->getAttributes(), $normalizedCols),
                        true
                    );
                }

                $result[$id] = $model;
            }

            if ($inserts !== []) {
                $memberKey = $this->prefix("members:model:{{$classKey}}");
                $this->connection()->pipeline(function ($pipe) use ($inserts, $memberKey) {
                    foreach ($inserts as $key => $value) {
                        $pipe->setex($key, $this->ttl, $value);
                        $pipe->sadd($memberKey, $key);
                    }
                });
            }
        }

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($result[$id])) {
                $ordered[] = $result[$id];
            }
        }

        return $ordered;
    }

    public function currentVersion(string $modelClass): int
    {
        $classKey = $this->classKey($modelClass);

        if (array_key_exists($classKey, $this->versionLocal)) {
            return $this->versionLocal[$classKey];
        }

        $value = $this->connection()->get($this->prefix("ver:{{$classKey}}:"));

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

        foreach ($invalidations as $modelClass) {
            $this->doInvalidateVersion($modelClass);
        }

        if (!empty($deletes)) {
            $this->asyncDel(array_map(fn($k) => $this->prefix($k), $deletes));
        }

        foreach ($flushes as $modelClass) {
            $this->flushModel($modelClass);
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

            $script = "if redis.call('SET', KEYS[1], 1, 'EX', ARGV[1], 'NX') then return redis.call('INCR', KEYS[2]) end return nil";
            $newVer = $this->connection()->eval($script, 2, $cooldownKey, $verKey, $this->cooldown);

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
        return self::$classKeyCache[$class] ??= (
            array_search($class, Relation::morphMap(), true)
                ?: strtolower(class_basename($class))
        );
    }

    public function modelKey(string $modelClass, int|string $id): string
    {
        return 'model:{' . $this->classKey($modelClass) . '}:' . $id;
    }

    public function flushInstance(Model $model): void
    {
        $conn  = $model->getConnectionName();
        $class = $model::class;
        $key   = $this->modelKey($class, $model->getKey());

        if ($conn !== null && DB::connection($conn)->transactionLevel() > 0) {
            $this->deleteQueue[$conn][$key] = true;
            $this->invalidationQueue[$conn][$class] = true;
            return;
        }

        $this->connection()->pipeline(function ($pipe) use ($key, $class) {
            $pipe->del($this->prefix($key));
            
            $classKey = $this->classKey($class);
            if ($this->cooldown > 0) {
                $cooldownKey = $this->prefix("cooldown:{{$classKey}}:");
                $verKey = $this->prefix("ver:{{$classKey}}:");
                $script = "if redis.call('SET', KEYS[1], 1, 'EX', ARGV[1], 'NX') then return redis.call('INCR', KEYS[2]) end return nil";
                $pipe->eval($script, 2, $cooldownKey, $verKey, $this->cooldown);
            } else {
                $pipe->incr($this->prefix("ver:{{$classKey}}:"));
            }
        });

        unset($this->versionLocal[$this->classKey($class)]);
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

    public function flushAll(): int
    {
        $patterns = ['query:*', 'model:*', 'members:model:*', 'ver:*', 'agg:*', 'count:*', 'pivot:*', 'through:*', 'cooldown:*', 'building:*'];
        $total = 0;
        $connection = $this->connection();
        $client = $connection->client();

        if ($this->cluster && method_exists($client, '_masters')) {
            foreach ($client->_masters() as $node) {
                foreach ($patterns as $pattern) {
                    $it = null;
                    while ($keys = $client->scan($it, $node, $this->prefix($pattern), 1000)) {
                        $total += count($keys);
                        $this->asyncDel($keys);
                    }
                }
            }
            return $total;
        }

        // Fallback for Predis or Single Node
        foreach ($patterns as $pattern) {
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

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds): array
    {
        $parentKey = $this->classKey($parentClass);
        $relatedKey = $this->classKey($relatedClass);

        $parentVersion = $this->currentVersion($parentClass);
        $relatedVersion = $this->currentVersion($relatedClass);

        $prefix = "pivot:{{$parentKey}}:{$relatedKey}:{$relation}:v{$parentVersion}:v{$relatedVersion}:";
        
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
        return $this->keyPrefix . $key;
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
