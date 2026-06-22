<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Values\QueryCacheResult;

final class NormalizedCacheReader
{
    use SlottedQueryAccess;

    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly int $staleVersionDepth,
        private readonly int $stampedeWaitMs = 200,
        private readonly bool $slotting = false,
    ) {}

    public function fetch(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): QueryCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);

        if (empty($depClasses) && empty($depTableKeys)) {
            $lockToken = $this->versions->buildLockToken();
            $result = $this->luaFetchVersionedQuery($classKey, $hash, $tag, $lockToken);

            $version = $this->versions->normalizeVersion($result[1]);
            $queryKey = $this->keys->queryKey($classKey, $tag, $version, $hash);
            $buildingKey = $this->keys->buildingPrefix($classKey) . $hash;
            [$status, $ids] = $this->resolveIds($result, $queryKey);

            return $this->toQueryResult(
                $status, $queryKey, $buildingKey, (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $lockToken),
                [$this->keys->verKey($classKey)],
                [(string) $version],
                $ids,
                is_array($ids) ? $this->fetchModels($classKey, $ids) : null
            );
        }

        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->keys->queryPrefix($classKey, $tag);
        $lockToken = $this->versions->buildLockToken();

        if ($this->slotting) {
            return $this->fetchSlotted($classKey, $hash, $queryPrefix, $versionKeys, $scheduledKeys, $lockToken);
        }

        $result = $this->luaFetchMultiVersionedQuery(
            $versionKeys, $scheduledKeys, $queryPrefix,
            $this->keys->buildingPrefix($classKey),
            $hash, $lockToken
        );

        $seg = (string) ($result[1] ?? '');
        $queryKey = $queryPrefix . $seg . ':' . $hash;
        $buildingKey = $this->keys->buildingPrefix($classKey) . $seg . ':' . $hash;
        $expectedVersions = $this->keys->versionsFromSegment($seg);
        [$status, $ids] = $this->resolveIds($result, $queryKey);

        return $this->toQueryResult(
            $status, $queryKey, $buildingKey, (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $lockToken),
            $versionKeys, $expectedVersions,
            $ids,
            is_array($ids) ? $this->fetchModels($classKey, $ids) : null
        );
    }

    private function fetchSlotted(
        string $classKey, string $hash, string $queryPrefix,
        array $versionKeys, array $scheduledKeys, string $lockToken
    ): QueryCacheResult {
        [$queryKey, $buildingKey, $expectedVersions] = $this->resolveSlotKeys($classKey, $hash, $queryPrefix, $versionKeys, $scheduledKeys);

        $raw = $this->store->getRaw($queryKey);
        $parsed = $raw !== null ? json_decode($raw, true) : null;

        if ($raw !== null && (!is_array($parsed) || !array_is_list($parsed))) {
            $this->store->delete($queryKey);
            $parsed = null;
        }

        if ($parsed === null) {
            return $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)
                ? new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions)
                : new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        }

        if (empty($parsed)) {
            return new QueryCacheResult(CacheStatus::Empty, $queryKey, [], [], null, null, [], []);
        }

        return new QueryCacheResult(CacheStatus::Hit, $queryKey, $parsed, $this->fetchModels($classKey, $parsed), null, null, [], []);
    }

    private function resolveIds(array $result, string $queryKey): array
    {
        if (($result[0] ?? null) !== 'hit_raw') {
            $status = LuaStatus::fromLua($result[0] ?? null);

            return [$status, $status->servesData() ? $result[2] : null];
        }

        $ids = json_decode($result[2], true);

        if (!is_array($ids) || !array_is_list($ids)) {
            $this->store->delete($queryKey);

            return [LuaStatus::Corrupt, null];
        }

        if (empty($ids)) {
            return [LuaStatus::Empty, []];
        }

        return [LuaStatus::Hit, $ids];
    }

    private function toQueryResult(
        LuaStatus $status,
        string $queryKey,
        string $buildingKey,
        string $lockToken,
        array $versionKeys,
        array $expectedVersions,
        mixed $ids = null,
        ?array $models = null
    ): QueryCacheResult {
        return match ($status) {
            LuaStatus::Hit => new QueryCacheResult(CacheStatus::Hit, $queryKey, $ids, $models ?? [], null, null, [], []),
            LuaStatus::Stale => new QueryCacheResult(CacheStatus::Stale, null, $ids, $models ?? [], null, null, [], []),
            LuaStatus::Empty => new QueryCacheResult(CacheStatus::Empty, $queryKey, [], [], null, null, [], []),
            LuaStatus::Miss => new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions),
            LuaStatus::Building => new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
            LuaStatus::Corrupt => $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)
                ? new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions)
                : new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
        };
    }

    public function store(string $key, array $ids, ?int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken): void
    {
        $ids = array_map('strval', $ids);
        $ttl ??= $this->queryTtl;

        if ($this->slotting && !empty($versionKeys)) {
            $this->storeSlottingGuarded($key, json_encode($ids, JSON_THROW_ON_ERROR), $ttl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken);

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
                    [json_encode($ids, JSON_THROW_ON_ERROR), $buildingToken ?? '']
                )
            );

            return;
        }

        if ($buildingKey === null) {
            return;
        }

        $this->store->storeRawAndRelease(
            $key,
            json_encode($ids, JSON_THROW_ON_ERROR),
            $ttl,
            $buildingKey,
            $this->keys->buildingToWakeKey($buildingKey),
            $buildingToken
        );
    }

    public function waitForBuild(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): ?QueryCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);
        $this->store->brpop($this->keys->wakePrefix($classKey) . $hash, $this->stampedeWaitMs / 1000.0);

        $result = $this->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    private function luaFetchVersionedQuery(string $classKey, string $hash, ?string $tag, string $lockToken): mixed
    {
        return $this->store->script(RedisScripts::get('fetch_versioned_query'), [
            $this->keys->verKey($classKey),
            $this->keys->scheduledKey($classKey),
            $this->keys->queryPrefix($classKey, $tag),
            $this->keys->buildingPrefix($classKey),
        ], [$hash, (int) floor(microtime(true) * 1000), $this->buildingLockTtl, $this->staleVersionDepth, $lockToken]);
    }

    private function luaFetchMultiVersionedQuery(
        array $versionKeys, array $scheduledKeys,
        string $queryPrefix, string $buildingPrefix,
        string $hash, string $lockToken,
    ): mixed {
        return $this->store->script(
            RedisScripts::get('fetch_multi_versioned_query'),
            array_merge($versionKeys, $scheduledKeys, [$queryPrefix, $buildingPrefix]),
            [$hash, (int) floor(microtime(true) * 1000), $this->buildingLockTtl, $lockToken]
        );
    }
}
