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
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly int $stampedeWaitMs = 200,
        private readonly int $wakeTokenCount = 64,
    ) {}

    public function fetch(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): QueryCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->keys->queryPrefix($classKey, $tag);
        $lockToken = $this->versions->buildLockToken();

        $result = $this->store->script(
            RedisScripts::get('fetch_versioned_payload'),
            array_merge($versionKeys, $scheduledKeys, [
                $queryPrefix,
                $this->keys->buildingPrefix($classKey),
                $this->keys->wakePrefix($classKey),
            ]),
            [$hash, $hash, (int) floor(microtime(true) * 1000), $this->buildingLockTtl, $lockToken]
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

    private function resolveIds(array $result, string $queryKey): array
    {
        $status = LuaStatus::fromLua($result[0] ?? null);

        if (!$status->hasPayload()) {
            return [$status, null];
        }

        if (!isset($result[2])) {
            return [LuaStatus::Corrupt, null];
        }

        $ids = $this->resolveIdsPayload($result[2], $queryKey);
        if ($ids === null) {
            return [LuaStatus::Corrupt, null];
        }

        if (empty($ids)) {
            return [LuaStatus::Empty, []];
        }

        return [$status, $ids];
    }

    private function resolveIdsPayload(mixed $payload, string $queryKey): ?array
    {
        $ids = is_string($payload) ? json_decode($payload, true) : $payload;

        if (!is_array($ids) || !array_is_list($ids)) {
            $this->store->delete($queryKey);

            return null;
        }

        return $ids;
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
            LuaStatus::Empty => new QueryCacheResult(CacheStatus::Empty, $queryKey, [], [], null, null, [], []),
            LuaStatus::Miss => new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions),
            LuaStatus::Building => new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
            LuaStatus::Corrupt => $this->claimMissAfterCorruptHit($queryKey, $buildingKey, $lockToken, $versionKeys, $expectedVersions),
        };
    }

    private function claimMissAfterCorruptHit(
        string $queryKey,
        string $buildingKey,
        string $lockToken,
        array $versionKeys,
        array $expectedVersions
    ): QueryCacheResult {
        if ($this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
            $this->store->delete($this->keys->buildingToWakeKey($buildingKey));

            return new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions);
        }

        return new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
    }

    public function store(string $key, array $ids, ?int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken): void
    {
        $ids = array_map('strval', $ids);
        $ttl ??= $this->queryTtl;

        if (!empty($versionKeys)) {
            $this->store->script(
                RedisScripts::get('store_many_versioned'),
                array_merge($versionKeys, [
                    $key,
                    $buildingKey ?? '',
                    $buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : '',
                ]),
                array_merge(
                    [(string) count($versionKeys), '1', (string) $ttl],
                    $expectedVersions,
                    [json_encode($ids, JSON_THROW_ON_ERROR), $buildingToken ?? '', (string) $this->wakeTokenCount]
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

    private function fetchModels(string $classKey, array $ids): array
    {
        if ($ids === []) {
            return [];
        }

        $modelPrefix = $this->keys->modelPrefix($classKey);

        return $this->store->getMany(array_map(static fn($id) => $modelPrefix . $id, $ids));
    }
}
