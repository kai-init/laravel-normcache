<?php

namespace NormCache;

use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Support\Collection;
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

    /** @var array<string, int> L1 in-process version cache, keyed by classKey */
    protected array $versionLocal = [];

    public function __construct(
        protected string $redisConnection,
        protected int $ttl,
        protected int $queryTtl,
        protected string $keyPrefix,
        protected int $cooldown,
    ) {}

    public function isEnabled(): bool
    {
        return config('normcache.enabled', true);
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

        $this->connection()->pipeline(function ($pipe) use ($pairs, $ttl) {
            foreach ($pairs as $key => $value) {
                $pipe->setex($this->prefix($key), $ttl, $this->serialize($value));
            }
        });
    }

    public function getModels(array $ids, string $modelClass): Collection
    {
        if ($ids === []) {
            return collect();
        }

        $classKey = $this->classKey($modelClass);

        $keys = array_map(
            fn ($id) => $this->prefix("model:$classKey:$id"),
            $ids
        );

        $raw = $this->connection()->mget($keys);
        $cached = array_combine($ids, $raw);

        $missed = [];
        $result = [];

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

            $model = (new $modelClass)->newInstance([], true);
            $model->exists = true;
            $model->setRawAttributes($attrs, true);

            $result[$id] = $model;
        }

        $hitIds = array_diff($ids, $missed);
        if ($hitIds !== []) {
            event(new ModelCacheHit($modelClass, array_values($hitIds)));
        }
        if ($missed !== []) {
            event(new ModelCacheMiss($modelClass, $missed));
        }

        if ($missed !== []) {
            $loaded = $modelClass::query()
                ->withoutCache()
                ->withoutGlobalScope(SoftDeletingScope::class)
                ->findMany($missed)
                ->keyBy('id');

            $inserts = [];

            foreach ($loaded as $id => $model) {
                $classKey = $this->classKey($modelClass);
                $result[$id] = $model;
                $inserts[$this->prefix("model:$classKey:$id")] = $this->serialize($model->getAttributes());
            }

            if ($inserts !== []) {
                $this->connection()->pipeline(function ($pipe) use ($inserts) {
                    foreach ($inserts as $key => $value) {
                        $pipe->setex($key, $this->ttl, $value);
                    }
                });
            }
        }

        return collect($ids)
            ->map(fn ($id) => $result[$id] ?? null)
            ->filter()
            ->values();
    }

    public function currentVersion(string $modelClass): int
    {
        $classKey = $this->classKey($modelClass);

        if (array_key_exists($classKey, $this->versionLocal)) {
            return $this->versionLocal[$classKey];
        }

        $value = $this->connection()->get($this->prefix('ver:' . $classKey));

        return $this->versionLocal[$classKey] = $value !== null ? (int) $value : 0;
    }

    public function flushVersionLocal(): void
    {
        $this->versionLocal = [];
    }

    public function invalidateVersion(string $modelClass, ?string $connectionName = null): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            $this->invalidationQueue[$connectionName][$modelClass] = true;
            return;
        }

        $this->doInvalidateVersion($modelClass);
    }

    public function deferDelete(string $key, ?string $connectionName = null): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            $this->deleteQueue[$connectionName][$key] = true;
            return;
        }

        $this->connection()->del($this->prefix($key));
    }

    public function commitPending(string $connectionName): void
    {
        $invalidations = array_keys($this->invalidationQueue[$connectionName] ?? []);
        $deletes = array_keys($this->deleteQueue[$connectionName] ?? []);

        unset($this->invalidationQueue[$connectionName], $this->deleteQueue[$connectionName]);

        foreach ($invalidations as $modelClass) {
            $this->doInvalidateVersion($modelClass);
        }

        if ($deletes !== []) {
            $this->asyncDel(array_map(fn($k) => $this->prefix($k), $deletes));
        }
    }

    public function discardPending(string $connectionName): void
    {
        unset($this->invalidationQueue[$connectionName], $this->deleteQueue[$connectionName]);
    }

    protected function doInvalidateVersion(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        if ($this->cooldown > 0) {
            $cooldownKey = $this->prefix("cooldown:$classKey");

            if (!$this->connection()->set($cooldownKey, 1, 'EX', $this->cooldown, 'NX')) {
                return;
            }
        }

        $this->versionLocal[$classKey] = (int) $this->connection()->incr(
            $this->prefix('ver:' . $classKey)
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
        return 'model:' . $this->classKey($modelClass) . ':' . $id;
    }

    public function flushModel(string $modelClass): void
    {
        $classKey = $this->classKey($modelClass);

        $this->doInvalidateVersion($modelClass);

        foreach (['query', 'model', 'agg', 'count'] as $namespace) {
            $keys = $this->scan($this->prefix("{$namespace}:{$classKey}:*"));
            $this->asyncDel($keys);
        }
    }

    public function flushAll(): int
    {
        $patterns = ['query:*', 'model:*', 'ver:*', 'agg:*', 'count:*', 'cooldown:*', 'building:*'];
        $total = 0;

        foreach ($patterns as $pattern) {
            $keys = $this->scan($this->prefix($pattern));
            $total += count($keys);
            $this->asyncDel($keys);
        }

        return $total;
    }

    protected function connection(): Connection
    {
        return $this->connection ??= Redis::connection($this->redisConnection);
    }

    protected function asyncDel(array $prefixedKeys): void
    {
        foreach (array_chunk($prefixedKeys, 1000) as $chunk) {
            try {
                $this->connection()->unlink(...$chunk);
            } catch (\Throwable) {
                $this->connection()->del(...$chunk);
            }
        }
    }

    protected function scan(string $pattern): array
    {
        $connection = $this->connection();

        $cursor = $connection instanceof PhpRedisConnection
            && version_compare(phpversion('redis'), '6.1.0', '>=')
                ? null
                : '0';

        $keys = [];

        do {
            $result = $connection->scan($cursor, ['match' => $pattern, 'count' => 100]);

            if (!is_array($result)) {
                break;
            }

            [$cursor, $batch] = $result;
            array_push($keys, ...$batch);
        } while ($cursor);

        return $keys;
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

        return serialize($value);
    }

    protected function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return $value;
        }

        return unserialize($value);
    }
}
