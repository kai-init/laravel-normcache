<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;

final class VersionTracker
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
    ) {}

    public function currentVersion(string $modelClass): int
    {
        return $this->normalizeVersion(
            $this->fetchVersionWithCooldown($this->keys->classKey($modelClass))
        );
    }

    public function currentTableVersion(string $connectionName, string $table): int
    {
        return $this->normalizeVersion(
            $this->fetchVersionWithCooldown($this->keys->tableKey($connectionName, $table))
        );
    }

    public function resolveVersions(array $versionKeys, array $scheduledKeys): array
    {
        $script = RedisScripts::get('fetch_version_with_cooldown');
        $nowMs = (string) (int) floor(microtime(true) * 1000);
        $map = [];

        foreach ($versionKeys as $i => $verKey) {
            if (!isset($map[$verKey])) {
                $map[$verKey] = (string) ($this->store->eval($script, [$verKey, $scheduledKeys[$i]], [$nowMs]) ?? '0');
            }
        }

        return $map;
    }

    public function expectedVersions(array $versionKeys, array $resolvedVersions): array
    {
        return array_map(static fn($key) => $resolvedVersions[$key], $versionKeys);
    }

    public function versionsStillMatch(array $versionKeys, array $expectedVersions): bool
    {
        foreach ($this->store->getRawMany($versionKeys) as $i => $version) {
            if ((string) ($version ?? '0') !== (string) $expectedVersions[$i]) {
                return false;
            }
        }

        return true;
    }

    public function buildLockToken(): string
    {
        return bin2hex(random_bytes(16));
    }

    public function normalizeVersion(mixed $value = null): int
    {
        return $value !== null ? (int) $value : 0;
    }

    private function fetchVersionWithCooldown(string $classKey): mixed
    {
        return $this->store->eval(
            RedisScripts::get('fetch_version_with_cooldown'),
            [$this->keys->verKey($classKey), $this->keys->scheduledKey($classKey)],
            [(string) (int) floor(microtime(true) * 1000)]
        );
    }
}
