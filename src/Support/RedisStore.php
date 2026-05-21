<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
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
    // Scalar operations
    // -------------------------------------------------------------------------

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
    // Bulk operations
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

    // -------------------------------------------------------------------------
    // Compound / set operations
    // -------------------------------------------------------------------------

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

    public function asyncDel(array $prefixedKeys): void
    {
        if (empty($prefixedKeys)) {
            return;
        }

        if (!$this->cluster) {
            foreach (array_chunk($prefixedKeys, 1000) as $chunk) {
                $this->connection->unlink(...$chunk);
            }

            return;
        }

        foreach ($this->groupByTag($prefixedKeys) as $keys) {
            foreach (array_chunk($keys, 1000) as $chunk) {
                $this->connection->unlink(...$chunk);
            }
        }
    }

    public function flushByPatterns(array $patterns): int
    {
        $total = 0;

        foreach ($patterns as $pattern) {
            $keys = $this->connection->keys($this->prefix($pattern));
            if (!empty($keys)) {
                $total += count($keys);
                $this->asyncDel($keys);
            }
        }

        return $total;
    }

    // -------------------------------------------------------------------------
    // Lua eval
    // -------------------------------------------------------------------------

    /** Prefixes $keys before passing them to EVAL; $args are passed as-is. */
    public function eval(string $script, array $keys, array $args = []): mixed
    {
        $prefixedKeys = $this->keyPrefix !== '' ? array_map(fn($k) => $this->keyPrefix . $k, $keys) : $keys;

        return $this->connection->eval($script, count($prefixedKeys), ...$prefixedKeys, ...$args);
    }

    /** Deserialize an array of raw Redis values (as returned from MGET inside Lua). */
    public function deserializeMany(array $raw): array
    {
        return array_map(
            fn($value) => $value !== null && $value !== false ? $this->unserialize($value) : null,
            $raw
        );
    }

    public function prefix(string $key): string
    {
        return $this->keyPrefix !== '' ? $this->keyPrefix . $key : $key;
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

    private function serialize(mixed $value): mixed
    {
        if (is_numeric($value) && is_finite((float) $value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_serialize($value) : serialize($value);
    }

    private function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_unserialize($value) : unserialize($value);
    }
}
