<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheSpace;

final class VersionStore
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
    ) {}

    public function currentVersion(
        string $modelClass,
        ?CacheSpace $space = null,
        ?string $connection = null,
    ): int {
        return $this->normalizeVersion(
            $this->fetchVersionWithCooldown($this->keys->classKey($modelClass, $connection), $space)
        );
    }

    public function currentTableVersion(string $connectionName, string $table, ?CacheSpace $space = null): int
    {
        return $this->normalizeVersion(
            $this->fetchVersionWithCooldown($this->keys->tableKey($connectionName, $table), $space)
        );
    }

    public function bump(
        string $classKey,
        CacheConfig $config,
        ?CacheSpace $space = null,
    ): void {
        if ($config->cooldown <= 0) {
            $this->forceBump($classKey, $this->versionTtl($config), $space);

            return;
        }

        [$versionKey, $scheduledKey] = $this->keys->versionKeyPair($classKey, $space);
        $this->store->fetchVersionWithCooldown($versionKey, $scheduledKey);

        $dueAtMs = (int) floor(microtime(true) * 1000) + ($config->cooldown * 1000);
        $this->store->setNxEx(
            $scheduledKey,
            (string) $dueAtMs,
            $config->cooldown + $this->versionTtl($config),
        );
    }

    public function forceBump(
        string $classKey,
        int $ttl,
        ?CacheSpace $space = null,
    ): int {
        return $this->store->incrementAndExpire(
            $this->keys->verKey($classKey, $space),
            $ttl,
        );
    }

    public function versionTtl(CacheConfig $config): int
    {
        return max($config->ttl, $config->queryTtl) * 2;
    }

    public function buildLockToken(): string
    {
        return bin2hex(random_bytes(16));
    }

    public function normalizeVersion(mixed $value = null): int
    {
        return $value !== null ? (int) $value : 0;
    }

    private function fetchVersionWithCooldown(string $classKey, ?CacheSpace $space = null): mixed
    {
        [$versionKey, $scheduledKey] = $this->keys->versionKeyPair($classKey, $space);

        return $this->store->fetchVersionWithCooldown(
            $versionKey,
            $scheduledKey,
        );
    }
}
