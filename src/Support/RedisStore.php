<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Predis\Cluster\RedisStrategy;
use Predis\NotSupportedException;

final class RedisStore
{
    private Connection $connection;

    private CacheSerializer $serializer;

    /** @var array<string, string> SHA1 cache — populated on first use of each script */
    private static array $shas = [];

    public function __construct(
        string $redisConnection,
        private int $wakeTokenCount = 64,
    ) {
        $this->serializer = new CacheSerializer;
        $this->connection = Redis::connection($redisConnection);
    }

    // -------------------------------------------------------------------------
    // Operations — singular
    // -------------------------------------------------------------------------

    public function get(string $key): mixed
    {
        $value = $this->connection->get($key);

        return ($value !== null && $value !== false) ? $this->unserialize($value) : null;
    }

    // Returns the raw string value without deserialization (for JSON-encoded entries).
    public function getRaw(string $key): ?string
    {
        $value = $this->connection->get($key);

        return ($value !== null && $value !== false) ? $value : null;
    }

    /** @return list<?string> */
    public function getRawMany(array $keys): array
    {
        return $this->mgetValues($keys, unserialize: false);
    }

    public function set(string $key, mixed $value, int $ttl): void
    {
        $this->connection->setex($key, $ttl, $this->serialize($value));
    }

    // Set a raw string value without serialization.
    public function setRaw(string $key, string $value, int $ttl): void
    {
        $this->connection->setex($key, $ttl, $value);
    }

    /** @param  list<string>  $values */
    public function addToSet(string $key, array $values): int
    {
        if ($values === []) {
            return 0;
        }

        return (int) $this->connection->command('sadd', [$key, ...$values]);
    }

    /** @return list<string> */
    public function setMembers(string $key): array
    {
        $members = $this->connection->command('smembers', [$key]);

        if (!is_array($members)) {
            return [];
        }

        return array_values(array_filter($members, 'is_string'));
    }

    // SET NX EX — returns true if the lock was claimed.
    public function setNxEx(string $key, string $value, int $ttl): bool
    {
        $result = $this->script(
            "return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))",
            [$key],
            [$value, (string) $ttl]
        );

        return $result !== null && $result !== false;
    }

    public function delete(string|array $keys): void
    {
        $keys = array_values(array_filter((array) $keys, fn($k) => $k !== ''));

        if ($keys === []) {
            return;
        }

        $this->del($keys);
    }

    // DEL building key + LPUSH/EXPIRE wake key atomically when the token still owns the lock.
    public function releaseBuilding(string $buildingKey, string $wakeKey, ?string $token = null): bool
    {
        $keys = $wakeKey !== '' ? [$buildingKey, $wakeKey] : [$buildingKey];

        return (bool) $this->script(
            RedisScripts::get('release_building'),
            $keys,
            [$token ?? '', (string) $this->wakeTokenCount]
        );
    }

    public function storeSerializedAndRelease(string $key, mixed $value, int $ttl, ?string $buildingKey = null, ?string $wakeKey = null, ?string $token = null): bool
    {
        return $this->storeRawAndRelease($key, $this->serialize($value), $ttl, $buildingKey, $wakeKey, $token);
    }

    public function storeRawAndRelease(string $key, string $value, int $ttl, ?string $buildingKey = null, ?string $wakeKey = null, ?string $token = null): bool
    {
        if ($buildingKey === null) {
            $this->connection->setex($key, $ttl, $value);

            return true;
        }

        $keys = $wakeKey !== null && $wakeKey !== '' ? [$key, $buildingKey, $wakeKey] : [$key, $buildingKey];

        return (bool) $this->script(
            RedisScripts::get('store_versioned_payload'),
            $keys,
            ['0', '1', (string) $ttl, $value, $token ?? '', (string) $this->wakeTokenCount]
        );
    }

    public function increment(string $key): int
    {
        return (int) $this->connection->incr($key);
    }

    public function incrementAndExpire(string $key, int $ttl): int
    {
        return (int) $this->script(
            "local v = redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])); return v",
            [$key],
            [(string) $ttl]
        );
    }

    /** Blocks until an item appears on the list key or the timeout expires. Returns true if woken.
     *  Requires Redis 6.0+ for sub-second precision; older Redis rounds the timeout up to 1s. */
    public function brpop(string $key, float $timeoutSeconds): bool
    {
        $result = $this->connection->brpop($key, $timeoutSeconds);

        return $result !== null && $result !== false;
    }

    // -------------------------------------------------------------------------
    // Operations — bulk
    // -------------------------------------------------------------------------

    public function getMany(array $keys): array
    {
        return $this->mgetValues($keys, unserialize: true);
    }

    // MGET in input order, with null for missing keys. All keys share the same hash tag so
    // a plain MGET is safe on Redis Cluster (same slot guaranteed).
    private function mgetValues(array $keys, bool $unserialize): array
    {
        if (empty($keys)) {
            return [];
        }

        $raw = $this->connection instanceof PredisClusterConnection
            ? array_map(fn($key) => $this->connection->get($key), $keys)
            : $this->connection->mget($keys);
        $values = [];

        foreach ($raw as $i => $value) {
            $values[$i] = $this->mgetValue($value, $unserialize);
        }

        return $values;
    }

    private function mgetValue(mixed $value, bool $unserialize): mixed
    {
        if ($value === null || $value === false) {
            return null;
        }

        return $unserialize ? $this->unserialize($value) : $value;
    }

    public function setMany(array $pairs, int $ttl): void
    {
        if (empty($pairs)) {
            return;
        }

        if ($this->isCluster()) {
            foreach ($pairs as $key => $value) {
                $this->connection->setex($key, $ttl, $this->serialize($value));
            }

            return;
        }

        $this->connection->pipeline(function ($pipe) use ($pairs, $ttl) {
            foreach ($pairs as $key => $value) {
                $pipe->setex($key, $ttl, $this->serialize($value));
            }
        });
    }

    // CAS write of model attribute entries; releases the build lock as part of the write when given.
    public function setManyIfVersion(
        array $attrsByKey,
        int $ttl,
        string $versionKey,
        int $expectedVersion,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
    ): void {
        if (empty($attrsByKey)) {
            if ($buildingKey !== null) {
                $this->releaseBuilding($buildingKey, $wakeKey ?? '', $token);
            }

            return;
        }

        $script = RedisScripts::get('store_model_attrs');
        $chunks = array_chunk($attrsByKey, 500, true);
        $lastChunk = array_key_last($chunks);

        foreach ($chunks as $i => $chunk) {
            $isLast = $i === $lastChunk;

            // Only the last chunk releases the build lock; trailing lock/wake keys are
            // present (and removed below if absent) only on that chunk.
            $keys = array_merge([$versionKey], array_keys($chunk));
            if ($isLast && $buildingKey !== null) {
                $keys[] = $buildingKey;
                if ($wakeKey !== null && $wakeKey !== '') {
                    $keys[] = $wakeKey;
                }
            }

            $this->script(
                $script,
                $keys,
                array_merge(
                    [(string) $expectedVersion, (string) $ttl, (string) count($chunk), $isLast ? ($token ?? '') : ''],
                    array_map(fn($attrs) => $this->serialize($attrs), array_values($chunk)),
                    [(string) $this->wakeTokenCount]
                )
            );
        }
    }

    public function asyncDel(array $prefixedKeys): void
    {
        if (empty($prefixedKeys)) {
            return;
        }

        foreach (array_chunk($prefixedKeys, 1000) as $chunk) {
            $this->del($chunk);
        }
    }

    private function del(array $keys): void
    {
        if ($this->connection instanceof PredisClusterConnection) {
            foreach ($keys as $key) {
                $this->connection->del($key);
            }

            return;
        }

        // Standalone Predis accepts Laravel's array form.
        if ($this->connection instanceof PredisConnection) {
            $this->connection->del($keys);

            return;
        }

        $this->connection->unlink($keys);
    }

    public function flushByPatterns(array $patterns): int
    {
        $total = 0;

        foreach ($patterns as $pattern) {
            $keys = $this->keysForPattern($pattern);

            if (!empty($keys)) {
                $total += count($keys);
                $this->asyncDel($keys);
            }
        }

        return $total;
    }

    // Runs a Lua script via EVALSHA, falling back to EVAL on NOSCRIPT. All KEYS must
    // share one hash slot (cluster); optional slots are omitted, never passed empty.
    public function script(string $script, array $keys, array $args = []): mixed
    {
        $n = count($keys);
        $allArgs = array_merge($keys, $args);

        $sha = self::$shas[$script] ??= sha1($script);

        try {
            // PredisClusterConnection extends PredisConnection, so this covers both.
            $shaArgs = $this->connection instanceof PredisConnection
                ? [$sha, $n, ...$allArgs]
                : [$sha, $allArgs, $n];

            $result = $this->connection->command('evalsha', $shaArgs);
        } catch (\Throwable $e) {
            if (
                !str_contains(strtolower($e->getMessage()), 'noscript') &&
                !($e instanceof NotSupportedException && str_contains($e->getMessage(), 'EVALSHA'))
            ) {
                throw $e;
            }

            return $this->connection->eval($script, $n, ...$allArgs);
        }

        // PhpRedis may return false with a NOSCRIPT last-error instead of throwing.
        if ($result === false && $this->connection instanceof PhpRedisConnection) {
            $lastError = strtolower((string) ($this->connection->client()->getLastError() ?? ''));
            if (str_contains($lastError, 'noscript')) {
                $this->connection->client()->clearLastError();

                return $this->connection->eval($script, $n, ...$allArgs);
            }
        }

        return $result;
    }

    // -------------------------------------------------------------------------
    // Serialization
    // -------------------------------------------------------------------------

    public function serialize(mixed $value): mixed
    {
        return $this->serializer->serialize($value);
    }

    public function unserialize(mixed $value): mixed
    {
        return $this->serializer->unserialize($value);
    }

    public function unserializeMany(array $raw): array
    {
        return $this->serializer->unserializeMany($raw);
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    public function isCluster(): bool
    {
        return $this->connection instanceof PhpRedisClusterConnection
            || $this->connection instanceof PredisClusterConnection;
    }

    private function isPhpRedis(): bool
    {
        // PhpRedisClusterConnection extends PhpRedisConnection, so this covers both.
        return $this->connection instanceof PhpRedisConnection;
    }

    private function connectionPrefix(): string
    {
        // *ClusterConnection extends *Connection, so these cover both cluster and standalone.
        if ($this->connection instanceof PhpRedisConnection) {
            return (string) $this->connection->client()->getOption(\Redis::OPT_PREFIX);
        }

        if ($this->connection instanceof PredisConnection) {
            $prefix = $this->connection->client()->getOptions()->prefix ?? null;
            if (is_object($prefix) && method_exists($prefix, 'getPrefix')) {
                return (string) $prefix->getPrefix();
            }
        }

        return '';
    }

    public function scanPattern(string $pattern): array
    {
        if ($this->connection instanceof PhpRedisClusterConnection) {
            $keys = $this->scanPhpRedisClusterKeys($pattern);
        } elseif ($this->connection instanceof PredisClusterConnection) {
            $keys = $this->scanPredisClusterKeys($pattern);
        } else {
            $keys = $this->scanKeys($pattern);
        }

        $connectionPrefix = $this->connectionPrefix();

        if ($connectionPrefix === '') {
            return $keys;
        }

        return array_map(
            static fn($k) => str_starts_with($k, $connectionPrefix) ? substr($k, strlen($connectionPrefix)) : $k,
            $keys
        );
    }

    private function keysForPattern(string $pattern): array
    {
        return $this->scanPattern($pattern);
    }

    private function scanKeys(string $pattern): array
    {
        $keys = [];
        $connectionPrefix = $this->connectionPrefix();

        $this->executeScan(
            function (&$cursor) use ($pattern, $connectionPrefix) {
                $p = $connectionPrefix . $pattern;

                return $this->isPhpRedis()
                    ? $this->connection->client()->scan($cursor, $p, 1000)
                    : $this->connection->scan($cursor, ['match' => $p, 'count' => 1000]);
            },
            function ($chunk) use (&$keys) {
                array_push($keys, ...$chunk);
            }
        );

        return $keys;
    }

    private function scanPredisClusterKeys(string $pattern): array
    {
        $targeted = $this->scanPredisClusterSlotKeys($pattern);
        if ($targeted !== null) {
            return $targeted;
        }

        $keys = [];
        $connectionPrefix = $this->connectionPrefix();

        foreach ($this->connection->client() as $node) {
            $this->executeScan(
                fn($cursor) => $node->scan($cursor, ['match' => $connectionPrefix . $pattern, 'count' => 1000]),
                function ($chunk) use (&$keys) {
                    array_push($keys, ...$chunk);
                }
            );
        }

        return array_values(array_unique($keys));
    }

    private function scanPredisClusterSlotKeys(string $pattern): ?array
    {
        $hashTag = $this->concreteHashTag($pattern);
        if ($hashTag === null) {
            return null;
        }

        try {
            $cluster = $this->connection->client()->getConnection();
            if (!method_exists($cluster, 'getConnectionBySlot')) {
                return null;
            }

            $slot = (new RedisStrategy)->getSlotByKey('{' . $hashTag . '}');
            $node = $cluster->getConnectionBySlot($slot);
            if (!is_object($node) || !method_exists($node, 'scan')) {
                return null;
            }
        } catch (\Throwable) {
            return null;
        }

        $keys = [];
        $connectionPrefix = $this->connectionPrefix();

        $this->executeScan(
            fn($cursor) => $node->scan($cursor, ['match' => $connectionPrefix . $pattern, 'count' => 1000]),
            function ($chunk) use (&$keys) {
                array_push($keys, ...$chunk);
            }
        );

        return array_values(array_unique($keys));
    }

    private function scanPhpRedisClusterKeys(string $pattern): array
    {
        $keys = [];
        $client = $this->connection->client();
        $connectionPrefix = $this->connectionPrefix();

        foreach ($client->_masters() as $node) {
            $this->executeScan(
                function (&$cursor) use ($client, $node, $pattern, $connectionPrefix) {
                    return $client->scan($cursor, $node, $connectionPrefix . $pattern, 1000);
                },
                function ($chunk) use (&$keys) {
                    array_push($keys, ...$chunk);
                }
            );
        }

        return $keys;
    }

    private function concreteHashTag(string $pattern): ?string
    {
        if (!preg_match('/\{([^{}]+)\}/', $pattern, $matches)) {
            return null;
        }

        $tag = $matches[1];

        return str_contains($tag, '*') || str_contains($tag, '?')
            ? null
            : $tag;
    }

    /**
     * @param  \Closure(mixed &): mixed  $scanner
     * @param  \Closure(array<mixed>): void  $processor
     */
    private function executeScan(\Closure $scanner, \Closure $processor): void
    {
        if ($this->isPhpRedis()) {
            // phpredis 6.x SCAN/SSCAN require null to start; updates cursor by reference.
            $cursor = null;
            while (true) {
                $chunk = $scanner($cursor);
                if (!empty($chunk)) {
                    $processor($chunk);
                }
                if (!$cursor) {
                    break;
                }
            }

            return;
        }

        // Predis returns [$cursor, $keys]; '0' signals completion.
        $cursor = '0';
        do {
            $result = $scanner($cursor);
            if (!is_array($result) || !isset($result[1])) {
                break;
            }
            [$cursor, $chunk] = $result;
            if (!empty($chunk)) {
                $processor($chunk);
            }
        } while ($cursor !== '0');
    }
}
