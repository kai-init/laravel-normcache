<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Values\ThroughCacheResult;

final class NormalizedThroughReader
{
    use SlottedQueryAccess;

    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly int $stampedeWaitMs = 200,
        private readonly bool $slotting = false,
    ) {}

    public function fetch(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): ThroughCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);

        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->keys->namespacedPrefix(CacheKeyBuilder::K_THROUGH, $classKey, $tag);

        $lockToken = $this->versions->buildLockToken();

        if ($this->usesSlotting()) {
            return $this->fetchSlotted($classKey, $hash, $queryPrefix, $versionKeys, $scheduledKeys, $lockToken);
        }

        $result = $this->luaFetchMultiVersionedThrough(
            $versionKeys, $scheduledKeys, $queryPrefix,
            $this->keys->buildingPrefix($classKey),
            $hash, $lockToken
        );

        $seg = (string) ($result[1] ?? '');
        $queryKey = $queryPrefix . $seg . ':' . $hash;
        $buildingKey = $this->keys->buildingPrefix($classKey) . $seg . ':' . $hash;
        $expectedVersions = $this->keys->versionsFromSegment($seg);
        [$status, $ids, $throughKeys] = $this->resolveIdsAndThroughKeys($result, $queryKey);
        $models = $status->servesData() ? $this->fetchModels($classKey, $ids) : null;

        return $this->toThroughResult(
            $status, $queryKey, $buildingKey, (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $lockToken),
            $versionKeys, $expectedVersions,
            $ids, $throughKeys, $models
        );
    }

    private function fetchSlotted(
        string $classKey, string $hash, string $queryPrefix,
        array $versionKeys, array $scheduledKeys, string $lockToken
    ): ThroughCacheResult {
        [$queryKey, $buildingKey, $expectedVersions] = $this->resolveSlotKeys($classKey, $hash, $queryPrefix, $versionKeys, $scheduledKeys);

        $raw = $this->store->getRaw($queryKey);
        $parsed = $raw !== null ? json_decode($raw, true) : null;

        if ($raw !== null && (!is_array($parsed) || !is_array($parsed['i'] ?? null) || !is_array($parsed['t'] ?? null))) {
            $this->store->delete($queryKey);
            $parsed = null;
        }

        if ($parsed === null) {
            return $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)
                ? new ThroughCacheResult(CacheStatus::Miss, $queryKey, null, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions)
                : new ThroughCacheResult(CacheStatus::Building, null, null, null, null, null, null, [], []);
        }

        $ids = $parsed['i'];
        $throughKeys = $parsed['t'];

        if (empty($ids)) {
            return new ThroughCacheResult(CacheStatus::Empty, $queryKey, [], [], [], null, null, [], []);
        }

        return new ThroughCacheResult(CacheStatus::Hit, $queryKey, $ids, $throughKeys, $this->fetchModels($classKey, $ids), null, null, [], []);
    }

    private function resolveIdsAndThroughKeys(array $result, string $queryKey): array
    {
        if (($result[0] ?? null) !== 'hit_raw') {
            $status = LuaStatus::fromLua($result[0] ?? null);

            return [
                $status,
                $status->servesData() ? ($result[2] ?? []) : null,
                $status->servesData() ? ($result[3] ?? []) : null,
            ];
        }

        $parsed = json_decode($result[2], true);

        if (!is_array($parsed) || !is_array($parsed['i'] ?? null) || !is_array($parsed['t'] ?? null)) {
            $this->store->delete($queryKey);

            return [LuaStatus::Corrupt, null, null];
        }

        if (empty($parsed['i'])) {
            return [LuaStatus::Empty, [], []];
        }

        return [LuaStatus::Hit, $parsed['i'], $parsed['t']];
    }

    private function toThroughResult(
        LuaStatus $status,
        string $queryKey,
        string $buildingKey,
        string $lockToken,
        array $versionKeys,
        array $expectedVersions,
        ?array $ids,
        ?array $throughKeys,
        ?array $models
    ): ThroughCacheResult {
        return match ($status) {
            LuaStatus::Hit => new ThroughCacheResult(CacheStatus::Hit, $queryKey, $ids, $throughKeys, $models ?? [], null, null, [], []),
            LuaStatus::Stale => new ThroughCacheResult(CacheStatus::Stale, null, $ids, $throughKeys, $models ?? [], null, null, [], []),
            LuaStatus::Empty => new ThroughCacheResult(CacheStatus::Empty, $queryKey, [], [], [], null, null, [], []),
            LuaStatus::Miss => new ThroughCacheResult(CacheStatus::Miss, $queryKey, null, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions),
            LuaStatus::Building => new ThroughCacheResult(CacheStatus::Building, null, null, null, null, null, null, [], []),
            LuaStatus::Corrupt => $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)
                ? new ThroughCacheResult(CacheStatus::Miss, $queryKey, null, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions)
                : new ThroughCacheResult(CacheStatus::Building, null, null, null, null, null, null, [], []),
        };
    }

    public function store(string $key, array $ids, array $throughKeys, ?int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken): void
    {
        $ids = array_map('strval', $ids);
        $ttl ??= $this->queryTtl;

        $payload = json_encode(['i' => $ids, 't' => $throughKeys], JSON_THROW_ON_ERROR);

        if ($this->usesSlotting() && !empty($versionKeys)) {
            $this->storeSlottingGuarded($key, $payload, $ttl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken);

            return;
        }

        if (!empty($versionKeys)) {
            $this->store->script(
                RedisScripts::get('store_if_versions_match_and_release'),
                array_merge($versionKeys, [
                    $key,
                    $buildingKey ?? '',
                    $buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : '',
                ]),
                array_merge(
                    [(string) count($versionKeys), (string) $ttl],
                    $expectedVersions,
                    [$payload, $buildingToken ?? '']
                )
            );

            return;
        }

        if ($buildingKey === null) {
            return;
        }

        $this->store->storeRawAndRelease(
            $key,
            $payload,
            $ttl,
            $buildingKey,
            $this->keys->buildingToWakeKey($buildingKey),
            $buildingToken
        );
    }

    public function waitForBuild(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): ?ThroughCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);
        $this->store->brpop($this->keys->wakePrefix($classKey) . $hash, $this->stampedeWaitMs / 1000.0);

        $result = $this->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    private function luaFetchMultiVersionedThrough(
        array $versionKeys, array $scheduledKeys,
        string $queryPrefix, string $buildingPrefix,
        string $hash, string $lockToken,
    ): mixed {
        return $this->store->script(
            RedisScripts::get('fetch_multi_versioned_through'),
            array_merge($versionKeys, $scheduledKeys, [$queryPrefix, $buildingPrefix]),
            [$hash, (int) floor(microtime(true) * 1000), $this->buildingLockTtl, $lockToken]
        );
    }
}
