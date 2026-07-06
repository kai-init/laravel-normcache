<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Predis\NotSupportedException;

final class RedisStore
{
    private Connection $connection;

    private CacheSerializer $serializer;

    private ?RedisScanner $scanner = null;

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

        return $this->storeVersionedPayload([$key => $value], $ttl, [], [], $buildingKey, $wakeKey, $token);
    }

    /** @param  array<string, mixed>  $entries  key => pre-encoded payload */
    public function storeVersionedPayload(
        array $entries,
        int $ttl,
        array $versionKeys,
        array $expectedVersions,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
    ): bool {
        $keys = array_merge($versionKeys, array_keys($entries));
        if ($buildingKey !== null) {
            $keys[] = $buildingKey;
            if ($wakeKey !== null && $wakeKey !== '') {
                $keys[] = $wakeKey;
            }
        }

        return (bool) $this->script(
            RedisScripts::get('store_versioned_payload'),
            $keys,
            array_merge(
                [(string) count($versionKeys), (string) count($entries), (string) $ttl],
                $expectedVersions,
                array_values($entries),
                [$token ?? '', (string) $this->wakeTokenCount]
            )
        );
    }

    public function fetchVersionedPayload(
        array $versionKeys,
        array $scheduledKeys,
        string $payloadPrefix,
        string $buildingPrefix,
        string $wakePrefix,
        string $hash,
        string $lockSuffix,
        string $lockToken,
        int $lockTtl,
        bool $cooldown,
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_versioned_payload'),
            array_merge($versionKeys, $cooldown ? $scheduledKeys : [], [$payloadPrefix, $buildingPrefix, $wakePrefix]),
            [
                $hash,
                $lockSuffix,
                (int) floor(microtime(true) * 1000),
                $lockTtl,
                $lockToken,
                (string) count($versionKeys),
                $cooldown ? '1' : '0',
            ]
        );
    }

    public function fetchVersionedPivotSegment(array $versionKeys, array $scheduledKeys): string
    {
        $result = $this->script(
            RedisScripts::get('fetch_versioned_pivot'),
            array_merge($versionKeys, $scheduledKeys),
            [(string) (int) floor(microtime(true) * 1000)]
        );

        return (string) ($result ?? '');
    }

    public function fetchBatchBuildStatus(array $keys, string $lockKey, string $wakeKey, string $token, int $lockTtl): array
    {
        return (array) $this->script(
            RedisScripts::get('fetch_batch_build_status'),
            [...$keys, $lockKey, $wakeKey],
            [$token, (string) $lockTtl]
        );
    }

    public function fetchVersionWithCooldown(string $verKey, string $scheduledKey): mixed
    {
        return $this->script(
            RedisScripts::get('fetch_version_with_cooldown'),
            [$verKey, $scheduledKey],
            [(string) (int) floor(microtime(true) * 1000)]
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
            $keys = $this->scanPattern($pattern);

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

    public function scanPattern(string $pattern): array
    {
        return ($this->scanner ??= new RedisScanner($this->connection))->scanPattern($pattern);
    }
}
