<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;

final class RedisStore
{
    private Connection $connection;

    private bool $igbinary;

    /** @var array<string, string> SHA1 cache — populated on first use of each script */
    private static array $shas = [];

    public function __construct(
        string $redisConnection,
        private string $keyPrefix,
        private bool $slotting,
        private string $slotPrefix = '',
    ) {
        $this->igbinary = extension_loaded('igbinary');
        $this->connection = Redis::connection($redisConnection);
    }

    // -------------------------------------------------------------------------
    // Operations — singular
    // -------------------------------------------------------------------------

    public function prefix(string $key): string
    {
        // Follow the expected order: slotPrefix then keyPrefix
        return $this->slotPrefix . $this->keyPrefix . $key;
    }

    public function get(string $key): mixed
    {
        $value = $this->connection->get($this->prefix($key));

        return ($value !== null && $value !== false) ? $this->unserialize($value) : null;
    }

    /** Returns the raw string value without deserialization (for JSON-encoded entries). */
    public function getRaw(string $key): ?string
    {
        $value = $this->connection->get($this->prefix($key));

        return ($value !== null && $value !== false) ? $value : null;
    }

    /** @return list<?string> */
    public function getRawMany(array $keys): array
    {
        if (empty($keys)) {
            return [];
        }

        if (!$this->slotting) {
            $raw = $this->connection->mget(array_map(fn($k) => $this->prefix($k), $keys));

            return array_map(fn($v) => ($v !== null && $v !== false) ? $v : null, $raw);
        }

        $results = [];

        foreach ($this->groupByTag($keys) as $groupKeys) {
            $raw = $this->connection->mget(array_map(fn($k) => $this->prefix($k), $groupKeys));

            $idx = 0;
            foreach ($groupKeys as $key) {
                $value = $raw[$idx++];
                $results[$key] = ($value !== null && $value !== false) ? $value : null;
            }
        }

        return array_map(fn($k) => $results[$k], $keys);
    }

    public function set(string $key, mixed $value, int $ttl): void
    {
        $this->connection->setex($this->prefix($key), $ttl, $this->serialize($value));
    }

    /** Set a raw string value without serialization. */
    public function setRaw(string $key, string $value, int $ttl): void
    {
        $this->connection->setex($this->prefix($key), $ttl, $value);
    }

    public function setJson(string $key, array $value, int $ttl): void
    {
        $this->connection->setex($this->prefix($key), $ttl, json_encode($value));
    }

    public function setNx(string $key, string $value): void
    {
        $this->connection->setnx($this->prefix($key), $value);
    }

    /** SET NX EX — returns true if the lock was claimed. */
    public function setNxEx(string $key, string $value, int $ttl): bool
    {
        $result = $this->eval(
            "return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))",
            [$key],
            [$value, (string) $ttl]
        );

        return $result !== null && $result !== false;
    }

    public function delete(string|array $keys): void
    {
        $keys = (array) $keys;
        if (empty($keys)) {
            return;
        }

        $prefixed = array_map(fn($k) => $this->prefix($k), $keys);
        $this->connection->del(...$prefixed);
    }

    /** DEL building key + LPUSH/EXPIRE wake key atomically when the token still owns the lock. */
    public function releaseBuilding(string $buildingKey, string $wakeKey, ?string $token = null): bool
    {
        return (bool) $this->eval(
            RedisScripts::get('release_building'),
            [$buildingKey, $wakeKey],
            [$token ?? '']
        );
    }

    public function storeSerializedAndRelease(string $key, mixed $value, int $ttl, ?string $buildingKey = null, ?string $wakeKey = null, ?string $token = null): bool
    {
        return $this->storeRawAndRelease($key, $this->serialize($value), $ttl, $buildingKey, $wakeKey, $token);
    }

    public function storeRawAndRelease(string $key, string $value, int $ttl, ?string $buildingKey = null, ?string $wakeKey = null, ?string $token = null): bool
    {
        if ($buildingKey === null) {
            $this->connection->setex($this->prefix($key), $ttl, $value);

            return true;
        }

        return (bool) $this->eval(
            RedisScripts::get('store_if_versions_match_and_release'),
            [$key, $buildingKey, $wakeKey ?? ''],
            ['0', (string) $ttl, $value, $token ?? '']
        );
    }

    public function increment(string $key): int
    {
        return (int) $this->connection->incr($this->prefix($key));
    }

    /** Blocks until an item appears on the list key or the timeout expires. Returns true if woken.
     *  Requires Redis 6.0+ for sub-second precision; older Redis rounds the timeout up to 1s. */
    public function brpop(string $key, float $timeoutSeconds): bool
    {
        $result = $this->connection->brpop($this->prefix($key), $timeoutSeconds);

        return $result !== null && $result !== false;
    }

    // -------------------------------------------------------------------------
    // Operations — bulk
    // -------------------------------------------------------------------------

    public function getMany(array $keys): array
    {
        if (empty($keys)) {
            return [];
        }

        if (!$this->slotting) {
            $prefixed = array_map(fn($k) => $this->prefix($k), $keys);

            return array_map(
                fn($v) => ($v !== null && $v !== false) ? $this->unserialize($v) : null,
                $this->connection->mget($prefixed)
            );
        }

        $results = [];

        foreach ($this->groupByTag($keys) as $groupKeys) {
            $prefixed = array_map(fn($k) => $this->prefix($k), $groupKeys);
            $raw = $this->connection->mget($prefixed);

            $idx = 0;
            foreach ($groupKeys as $key) {
                $value = $raw[$idx++];
                $results[$key] = ($value !== null && $value !== false) ? $this->unserialize($value) : null;
            }
        }

        return array_map(fn($k) => $results[$k], $keys);
    }

    public function setMany(array $pairs, int $ttl): void
    {
        if (empty($pairs)) {
            return;
        }

        if (!$this->slotting) {
            $this->connection->pipeline(function ($pipe) use ($pairs, $ttl) {
                foreach ($pairs as $key => $value) {
                    $pipe->setex($this->prefix($key), $ttl, $this->serialize($value));
                }
            });

            return;
        }

        foreach ($this->groupByTag(array_keys($pairs)) as $keys) {
            $this->connection->pipeline(function ($pipe) use ($keys, $pairs, $ttl) {
                foreach ($keys as $key) {
                    $pipe->setex($this->prefix($key), $ttl, $this->serialize($pairs[$key]));
                }
            });
        }
    }

    public function setManyTrackedIfVersion(array $attrsByKey, int $ttl, string $memberKey, string $versionKey, int $expectedVersion): void
    {
        if (empty($attrsByKey)) {
            return;
        }

        $script = RedisScripts::get('set_many_tracked_if_version');

        foreach (array_chunk($attrsByKey, 500, true) as $chunk) {
            $this->eval(
                $script,
                array_merge([$versionKey, $memberKey], array_keys($chunk)),
                array_merge(
                    [(string) $expectedVersion, (string) $ttl, (string) count($chunk)],
                    array_map(fn($attrs) => $this->serialize($attrs), array_values($chunk))
                )
            );
        }
    }

    /** Delete a key and remove it from a tracking set in one pipeline. */
    public function deleteFromSet(string $prefixedKey, string $prefixedMemberKey): void
    {
        $this->connection->pipeline(function ($pipe) use ($prefixedKey, $prefixedMemberKey) {
            $pipe->del($prefixedKey);
            $pipe->srem($prefixedMemberKey, $prefixedKey);
        });
    }

    public function sscanAndFlushSet(string $prefixedMemberKey): void
    {
        $cursor = version_compare(phpversion('redis'), '6.1.0', '>=') ? null : '0';

        do {
            $result = $this->connection->sscan($prefixedMemberKey, $cursor, ['match' => '*', 'count' => 1000]);

            if ($result === false) {
                break;
            }

            [$cursor, $members] = $result;

            if (!empty($members)) {
                $this->asyncDel($members);
            }
        } while ((int) $cursor !== 0);

        $this->connection->del($prefixedMemberKey);
    }

    public function asyncDel(array $prefixedKeys): void
    {
        if (empty($prefixedKeys)) {
            return;
        }

        if (!$this->slotting) {
            foreach (array_chunk($prefixedKeys, 1000) as $chunk) {
                $this->del($chunk);
            }

            return;
        }

        foreach ($this->groupByTag($prefixedKeys) as $keys) {
            foreach (array_chunk($keys, 1000) as $chunk) {
                $this->del($chunk);
            }
        }
    }

    private function del(array $keys): void
    {
        if ($this->connection instanceof PredisConnection) {
            // Predis 3.x doesn't register UNLINK
            $this->connection->del($keys);

            return;
        }

        $this->connection->unlink(...$keys);
    }

    public function flushByPatterns(array $patterns): int
    {
        $total = 0;
        $connectionPrefix = $this->connectionPrefix();

        foreach ($patterns as $pattern) {
            $keys = $this->keysForPattern($pattern);

            if ($connectionPrefix !== '') {
                $keys = array_map(
                    fn($key) => str_starts_with($key, $connectionPrefix)
                        ? substr($key, strlen($connectionPrefix))
                        : $key,
                    $keys
                );
            }

            if (!empty($keys)) {
                $total += count($keys);
                $this->asyncDel($keys);
            }
        }

        return $total;
    }

    /** Prefixes $keys before passing them to EVALSHA, falling back to EVAL on NOSCRIPT. */
    public function eval(string $script, array $keys, array $args = []): mixed
    {
        $prefixedKeys = array_map(fn($k) => $k === '' ? '' : $this->prefix($k), $keys);
        $n = count($prefixedKeys);
        $allArgs = array_merge($prefixedKeys, $args);

        $sha = self::$shas[$script] ??= sha1($script);

        try {
            $shaArgs = $this->connection instanceof PredisConnection
                ? [$sha, $n, ...$allArgs]
                : [$sha, $allArgs, $n];

            $result = $this->connection->command('evalsha', $shaArgs);
        } catch (\Throwable $e) {
            if (!str_contains(strtolower($e->getMessage()), 'noscript')) {
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
        if ((is_int($value) || is_float($value)) && is_finite((float) $value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_serialize($value) : serialize($value);
    }

    public function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return str_contains($value, '.') ? (float) $value : (int) $value;
        }

        if (is_string($value) && isset($value[0]) && $value[0] === "\x00") {
            return $this->igbinary ? igbinary_unserialize($value) : null;
        }

        // Check if it's a serialized string. PHP serialized strings start with
        // s:, i:, d:, b:, a:, O:, C:, R:, r:, N;
        if (is_string($value) && preg_match('/^[sidbaOCRrN]:|^[sidbaOCRrN];/', $value)) {
            try {
                return unserialize($value);
            } catch (\Throwable) {
                return $value;
            }
        }

        return $value;
    }

    public function unserializeMany(array $raw): array
    {
        return array_map(
            fn($value) => $value !== null && $value !== false ? $this->unserialize($value) : null,
            $raw
        );
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function groupByTag(array $keys): array
    {
        $groups = [];

        foreach ($keys as $key) {
            $tag = preg_match('/\{([^}]+)\}/', $key, $matches) ? $matches[1] : $key;
            $groups[$tag][] = $key;
        }

        return $groups;
    }

    private function connectionPrefix(): string
    {
        return match (true) {
            $this->connection instanceof PhpRedisConnection => $this->connection->_prefix(''),
            $this->connection instanceof PredisConnection => $this->predisPrefix($this->connection),
            default => '',
        };
    }

    private function predisPrefix(PredisConnection $connection): string
    {
        $prefix = $connection->getOptions()->prefix ?? null;

        if (is_object($prefix) && method_exists($prefix, 'getPrefix')) {
            return (string) $prefix->getPrefix();
        }

        return '';
    }

    private function keysForPattern(string $pattern): array
    {
        $pattern = $this->prefix($pattern);

        if ($this->connection instanceof PhpRedisClusterConnection) {
            return $this->scanPhpRedisClusterKeys($pattern);
        }

        if ($this->connection instanceof PredisClusterConnection) {
            return $this->scanPredisClusterKeys($pattern);
        }

        return $this->scanKeys($pattern);
    }

    private function scanKeys(string $pattern): array
    {
        $keys = [];
        $cursor = version_compare(phpversion('redis'), '6.1.0', '>=') ? null : '0';

        do {
            $result = $this->connection->scan($cursor, ['match' => $pattern, 'count' => 1000]);

            if ($result === false) {
                break;
            }

            $cursor = $this->appendScannedKeys($keys, $result);
        } while ((int) $cursor !== 0); // cast for compatibility

        return $keys;
    }

    private function appendScannedKeys(array &$keys, mixed $result): mixed
    {
        if (!is_array($result)) {
            return 0;
        }

        $cursor = $result[0] ?? 0;
        $chunk = $result[1] ?? [];

        if (is_array($chunk) && !empty($chunk)) {
            array_push($keys, ...$chunk);
        }

        return $cursor;
    }

    private function scanPredisClusterKeys(string $pattern): array
    {
        $keys = [];

        foreach ($this->connection->client() as $node) {
            $cursor = '0';

            do {
                [$cursor, $chunk] = $node->scan($cursor, ['match' => $pattern, 'count' => 1000]);

                if (!empty($chunk)) {
                    array_push($keys, ...$chunk);
                }
            } while ((int) $cursor !== 0);
        }

        return $keys;
    }

    private function scanPhpRedisClusterKeys(string $pattern): array
    {
        $keys = [];
        $defaultCursor = version_compare(phpversion('redis'), '6.1.0', '>=') ? null : '0';

        foreach ($this->connection->client()->_masters() as $node) {
            $cursor = $defaultCursor;

            do {
                $result = $this->connection->scan($cursor, [
                    'node' => $node,
                    'match' => $pattern,
                    'count' => 1000,
                ]);

                if ($result === false) {
                    break;
                }

                [$cursor, $chunk] = $result;
                $keys = [...$keys, ...$chunk];
            } while ((int) $cursor !== 0);
        }

        return $keys;
    }
}
