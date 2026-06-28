<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheSpace;

final class VersionTracker
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
    ) {}

    public function currentVersion(string $modelClass, ?CacheSpace $space = null): int
    {
        return $this->normalizeVersion(
            $this->fetchVersionWithCooldown($this->keys->classKey($modelClass), $space)
        );
    }

    public function currentTableVersion(string $connectionName, string $table, ?CacheSpace $space = null): int
    {
        return $this->normalizeVersion(
            $this->fetchVersionWithCooldown($this->keys->tableKey($connectionName, $table), $space)
        );
    }

    public function buildLockToken(): string
    {
        return hash('xxh3', microtime(true) . mt_rand());
    }

    public function normalizeVersion(mixed $value = null): int
    {
        return $value !== null ? (int) $value : 0;
    }

    private function fetchVersionWithCooldown(string $classKey, ?CacheSpace $space = null): mixed
    {
        return $this->store->script(
            RedisScripts::get('fetch_version_with_cooldown'),
            [$this->keys->verKey($classKey, $space), $this->keys->scheduledKey($classKey, $space)],
            [(string) (int) floor(microtime(true) * 1000)]
        );
    }
}
