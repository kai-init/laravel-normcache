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

    public function __construct(
        string $redisConnection,
        private string $keyPrefix,
        private bool $cluster,
    ) {
        $this->igbinary = extension_loaded('igbinary');
        $this->connection = Redis::connection($redisConnection);
    }

    // -------------------------------------------------------------------------
    // Operations — singular
    // -------------------------------------------------------------------------

    public function prefix(string $key): string
    {
        return $this->keyPrefix !== '' ? $this->keyPrefix . $key : $key;
    }

    public function get(string $key): mixed
    {
        $value = $this->connection->get($this->prefix($key));

        return $value !== null ? $this->unserialize($value) : null;
    }

    /** Returns the raw string value without deserialization (for JSON-encoded entries). */
    public function getRaw(string $key): string|false|null
    {
        return $this->connection->get($this->prefix($key));
    }

    public function getJson(string $key): ?array
    {
        $raw = $this->connection->get($this->prefix($key));
        if ($raw === null) {
            return null;
        }
        $decoded = json_decode($raw, true);

        return is_array($decoded) ? $decoded : null;
    }

    public function set(string $key, mixed $value, int $ttl): void
    {
        $this->connection->setex($this->prefix($key), $ttl, $this->serialize($value));
    }

    public function setJson(string $key, array $value, int $ttl): void
    {
        $this->connection->setex($this->prefix($key), $ttl, json_encode($value));
    }

    public function setNx(string $key, string $value): void
    {
        $this->connection->setnx($this->prefix($key), $value);
    }

    public function delete(string $key): void
    {
        $this->connection->del($this->prefix($key));
    }

    public function increment(string $key): int
    {
        return (int) $this->connection->incr($this->prefix($key));
    }

    // -------------------------------------------------------------------------
    // Operations — bulk
    // -------------------------------------------------------------------------

    public function getMany(array $keys): array
    {
        if (empty($keys)) {
            return [];
        }

        if (!$this->cluster) {
            $prefixed = $this->keyPrefix !== '' ? array_map(fn($k) => $this->keyPrefix . $k, $keys) : $keys;

            return array_map(
                fn($v) => $v !== null ? $this->unserialize($v) : null,
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

        if (!$this->cluster) {
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

    /**
     * Write model attribute entries in a pipeline, also adding their prefixed keys to a
     * Redis set (memberKey) so the whole group can be flushed atomically.
     */
    public function setManyTracked(array $attrsByKey, int $ttl, string $memberKey): void
    {
        if (empty($attrsByKey)) {
            return;
        }

        if (!$this->cluster) {
            $this->connection->pipeline(function ($pipe) use ($attrsByKey, $ttl, $memberKey) {
                $prefixedKeys = [];
                foreach ($attrsByKey as $key => $attrs) {
                    $p = $this->prefix($key);
                    $prefixedKeys[] = $p;
                    $pipe->setex($p, $ttl, $this->serialize($attrs));
                }
                $pipe->sadd($memberKey, ...$prefixedKeys);
                $pipe->expire($memberKey, $ttl);
            });

            return;
        }

        foreach ($this->groupByTag(array_keys($attrsByKey)) as $keys) {
            $this->connection->pipeline(function ($pipe) use ($keys, $attrsByKey, $ttl, $memberKey) {
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

    public function setManyTrackedIfVersion(array $attrsByKey, int $ttl, string $memberKey, string $versionKey, int $expectedVersion): void
    {
        if (empty($attrsByKey)) {
            return;
        }

        $script = <<<'LUA'
            local current = redis.call('GET', KEYS[1]) or '0'
            if current ~= ARGV[1] then return 0 end

            local ttl = tonumber(ARGV[2])
            local n = tonumber(ARGV[3])
            local members = {}

            for i = 1, n do
                local key = KEYS[2 + i]
                redis.call('SETEX', key, ttl, ARGV[3 + i])
                members[i] = key
            end

            redis.call('SADD', KEYS[2], unpack(members))
            redis.call('EXPIRE', KEYS[2], ttl)

            return n
        LUA;

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

    public function smembers(string $prefixedKey): array
    {
        return $this->connection->smembers($prefixedKey) ?: [];
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
                $this->del($members);
            }
        } while ((int) $cursor !== 0);

        $this->connection->del($prefixedMemberKey);
    }

    public function asyncDel(array $prefixedKeys): void
    {
        if (empty($prefixedKeys)) {
            return;
        }

        if (!$this->cluster) {
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

    /** Prefixes $keys before passing them to EVAL; $args are passed as-is. */
    public function eval(string $script, array $keys, array $args = []): mixed
    {
        $prefixedKeys = $this->keyPrefix !== '' ? array_map(fn($k) => $this->keyPrefix . $k, $keys) : $keys;

        return $this->connection->eval($script, count($prefixedKeys), ...$prefixedKeys, ...$args);
    }

    // -------------------------------------------------------------------------
    // Serialization
    // -------------------------------------------------------------------------

    public function serialize(mixed $value): mixed
    {
        if (is_numeric($value) && is_finite((float) $value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_serialize($value) : serialize($value);
    }

    public function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return $value;
        }

        if (isset($value[0]) && $value[0] === "\x00") {
            return $this->igbinary ? igbinary_unserialize($value) : null;
        }

        return unserialize($value);
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
        } while ((int) $cursor !== 0); // cast for competibility

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
            } while ($cursor !== 0);
        }

        return $keys;
    }
}
