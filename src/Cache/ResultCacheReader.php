<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\ResultCacheResult;

final class ResultCacheReader
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly int $stampedeWaitMs,
        private readonly bool $slotting = false,
        private readonly int $wakeTokenCount = 64,
        private readonly int $staleVersionDepth = 3,
    ) {}

    private function usesSlotting(): bool
    {
        return $this->store->requiresSlotting($this->slotting);
    }

    public function fetch(
        string $modelClass, array $depClasses, string $hash,
        ?string $tag, array $depTableKeys,
        string $namespace = CacheKeyBuilder::K_RESULT
    ): ResultCacheResult {
        $classKey = $this->keys->classKey($modelClass);
        $lockSuffix = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($classKey, $depClasses, $depTableKeys);
        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);
        $lockToken = $this->versions->buildLockToken();

        if ($this->usesSlotting()) {
            $resolvedVersions = $this->versions->resolveVersions($versionKeys, $scheduledKeys);
            $seg = $this->keys->versionSegment($versionKeys, $resolvedVersions);
            $expectedVersions = $this->versions->expectedVersions($versionKeys, $resolvedVersions);
            $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
            $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);

            $payload = $this->store->get($resultKey);
            $status = $payload !== null ? LuaStatus::Hit : LuaStatus::Miss;

            return $this->toResultCacheResult(
                $status, $resultKey, $buildingKey, $lockToken, $wakeKey,
                $versionKeys, $expectedVersions, $payload, true, false
            );
        }

        [$status, $seg, $payload, $claimedToken] = $this->luaFetchVersionedResult(
            $versionKeys, $scheduledKeys,
            $this->keys->namespacedPrefix($namespace, $classKey, $tag),
            $this->keys->buildingPrefix($classKey),
            $this->keys->wakePrefix($classKey),
            $hash, $lockSuffix, $lockToken
        );

        $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
        $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        return $this->toResultCacheResult(
            $status, $resultKey, $buildingKey, (string) ($claimedToken ?? $lockToken), $wakeKey,
            $versionKeys, $expectedVersions, $payload, false, $status === LuaStatus::Miss
        );
    }

    private function toResultCacheResult(
        LuaStatus $status,
        string $resultKey,
        string $buildingKey,
        string $lockToken,
        string $wakeKey,
        array $versionKeys,
        array $expectedVersions,
        mixed $payload = null,
        bool $payloadAlreadyUnserialized = true,
        bool $alreadyClaimed = false
    ): ResultCacheResult {
        if ($status === LuaStatus::Hit || $status === LuaStatus::Stale) {
            $unserialized = $payloadAlreadyUnserialized ? $payload : $this->store->unserialize($payload);
            if (is_array($unserialized)) {
                $cacheStatus = $status === LuaStatus::Hit ? CacheStatus::Hit : CacheStatus::Stale;
                $key = $status === LuaStatus::Hit ? $resultKey : null;

                return new ResultCacheResult($cacheStatus, $key, $unserialized, null, null, null, $versionKeys, $expectedVersions);
            }

            // Corrupt payload (not an array), treat as miss.
            $alreadyClaimed = false;
        }

        if ($status === LuaStatus::Building) {
            return new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        }

        // Standard miss or corrupt hit: attempt to claim building lock.
        if ($alreadyClaimed || $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
            if (!$alreadyClaimed) {
                $this->store->delete($wakeKey);
            }

            return new ResultCacheResult(CacheStatus::Miss, $resultKey, null, $buildingKey, $lockToken, $wakeKey, $versionKeys, $expectedVersions);
        }

        return new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
    }

    public function fetchPivot(
        string $parentClass, string $relatedClass, string $relation,
        array $parentIds, string $constraintHash, ?string $pivotTableKey
    ): PivotCacheResult {
        $parentKey = $this->keys->classKey($parentClass);
        $relatedKey = $this->keys->classKey($relatedClass);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($relatedKey, [], [$pivotTableKey ?? $parentKey]);

        if ($this->usesSlotting()) {
            $resolvedVersions = $this->versions->resolveVersions($versionKeys, $scheduledKeys);
            $seg = $this->keys->versionSegment($versionKeys, $resolvedVersions);
            $expectedVersions = $this->versions->expectedVersions($versionKeys, $resolvedVersions);
            $pivotKeys = [];
            foreach ($parentIds as $id) {
                $pivotKeys[] = $this->keys->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id);
            }

            return new PivotCacheResult(
                $seg,
                array_combine($parentIds, $this->store->getMany($pivotKeys)),
                $versionKeys,
                $expectedVersions,
            );
        }

        $seg = $this->luaFetchVersionedPivotSegment($versionKeys, $scheduledKeys);

        $pivotKeys = [];
        foreach ($parentIds as $id) {
            $pivotKeys[] = $this->keys->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id);
        }

        $data = array_combine($parentIds, $this->store->getMany($pivotKeys));
        $expectedVersions = $this->keys->versionsFromSegment($seg);
        $missed = array_keys(array_filter($data, fn($payload) => !is_array($payload)));

        if ($missed === []) {
            return new PivotCacheResult($seg, $data, $versionKeys, $expectedVersions);
        }

        [$lockKey, $wakeKey] = $this->pivotLockKeys($relatedKey, $relation, $constraintHash, $parentIds, $seg);
        $token = $this->versions->buildLockToken();

        $missedKeys = [];
        foreach ($missed as $id) {
            $missedKeys[] = $this->keys->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id);
        }

        $result = $this->store->script(
            RedisScripts::get('fetch_pivot_build_status'),
            [...$missedKeys, $lockKey, $wakeKey],
            [$token, (string) $this->buildingLockTtl]
        );

        $raw = $this->store->unserializeMany($result[2] ?? []);
        foreach ($missed as $i => $id) {
            if (isset($raw[$i]) && is_array($raw[$i])) {
                $data[$id] = $raw[$i];
            }
        }

        if (array_filter($data, fn($payload) => !is_array($payload)) === []) {
            return new PivotCacheResult($seg, $data, $versionKeys, $expectedVersions);
        }

        if ($result[0] === 'miss') {
            return new PivotCacheResult(
                $seg, $data, $versionKeys, $expectedVersions,
                CacheStatus::Miss, $lockKey, $token, $wakeKey
            );
        }

        return new PivotCacheResult(
            $seg, $data, $versionKeys, $expectedVersions,
            CacheStatus::Building
        );
    }

    public function waitForPivotBuild(
        string $parentClass, string $relatedClass, string $relation,
        array $parentIds, string $constraintHash, ?string $pivotTableKey
    ): ?PivotCacheResult {
        $relatedKey = $this->keys->classKey($relatedClass);
        [, $wakeKey] = $this->pivotLockKeys($relatedKey, $relation, $constraintHash, $parentIds, null);

        $this->store->brpop($wakeKey, $this->stampedeWaitMs / 1000.0);

        $result = $this->fetchPivot($parentClass, $relatedClass, $relation, $parentIds, $constraintHash, $pivotTableKey);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    // Lock key is segment-specific; the wake key is not, since waiters re-fetch to learn the current segment anyway.
    private function pivotLockKeys(string $relatedKey, string $relation, string $constraintHash, array $parentIds, ?string $seg): array
    {
        $sortedIds = $parentIds;
        sort($sortedIds);
        $lockSuffix = $this->keys->resultBuildIdentityHash('pivot', $relation, $constraintHash . ':' . implode(',', $sortedIds));

        return [
            $seg !== null ? $this->keys->resultBuildingKey($relatedKey, $seg, $lockSuffix) : null,
            $this->keys->wakeKey($relatedKey, $lockSuffix),
        ];
    }

    public function store(
        string $key, mixed $payload, ?string $buildingKey, ?int $ttl,
        ?string $wakeKey, array $versionKeys, array $expectedVersions, ?string $buildingToken
    ): bool {
        $ttl ??= $this->queryTtl;

        if ($versionKeys !== []) {
            return $this->storeEntry($key, $payload, $ttl, $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken);
        }

        return $this->store->storeSerializedAndRelease(
            $key, $payload, $ttl, $buildingKey,
            $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : null),
            $buildingToken
        );
    }

    public function storeMany(
        array $entries, int $ttl,
        array $versionKeys, array $expectedVersions,
        ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null
    ): bool {
        if (empty($entries)) {
            if ($this->usesSlotting() && $buildingKey !== null) {
                $this->store->releaseBuilding($buildingKey, $wakeKey ?? $this->keys->buildingToWakeKey($buildingKey), $buildingToken);
            }

            return true;
        }

        if ($this->usesSlotting()) {
            return $this->storeSlottingGuarded(
                fn() => $this->store->setMany($entries, $ttl),
                $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken
            );
        }

        return (bool) $this->store->script(
            RedisScripts::get('store_many_versioned'),
            array_merge($versionKeys, array_keys($entries), [
                $buildingKey ?? '',
                $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : ''),
            ]),
            array_merge(
                [(string) count($versionKeys), (string) count($entries), (string) $ttl],
                $expectedVersions,
                array_map(fn($p) => $this->store->serialize($p), array_values($entries)),
                [$buildingToken ?? '', (string) $this->wakeTokenCount]
            )
        );
    }

    public function storeEntry(
        string $key, mixed $payload, int $ttl,
        array $versionKeys, array $expectedVersions,
        ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null
    ): bool {
        if ($this->usesSlotting()) {
            return $this->storeSlottingGuarded(
                fn() => $this->store->set($key, $payload, $ttl),
                $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken
            );
        }

        return (bool) $this->store->script(
            RedisScripts::get('store_many_versioned'),
            array_merge($versionKeys, [
                $key,
                $buildingKey ?? '',
                $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : ''),
            ]),
            array_merge(
                [(string) count($versionKeys), '1', (string) $ttl],
                $expectedVersions,
                [$this->store->serialize($payload), $buildingToken ?? '', (string) $this->wakeTokenCount]
            )
        );
    }

    private function storeSlottingGuarded(
        callable $write,
        array $versionKeys, array $expectedVersions,
        ?string $buildingKey, ?string $wakeKey, ?string $buildingToken
    ): bool {
        if ($buildingKey !== null && $buildingToken !== null && $this->store->getRaw($buildingKey) !== $buildingToken) {
            return false;
        }

        $written = $this->versions->versionsStillMatch($versionKeys, $expectedVersions);
        if ($written) {
            $write();
        }

        if ($buildingKey !== null) {
            $this->store->releaseBuilding(
                $buildingKey,
                $wakeKey ?? $this->keys->buildingToWakeKey($buildingKey),
                $buildingToken
            );
        }

        return $written;
    }

    public function waitForBuild(
        string $modelClass, array $depClasses, string $hash,
        ?string $tag, array $depTableKeys,
        string $namespace = CacheKeyBuilder::K_RESULT
    ): ?ResultCacheResult {
        $classKey = $this->keys->classKey($modelClass);
        $wakeSuffix = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);
        $this->store->brpop($this->keys->wakePrefix($classKey) . $wakeSuffix, $this->stampedeWaitMs / 1000.0);

        $result = $this->fetch($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    private function luaFetchVersionedResult(
        array $versionKeys, array $scheduledKeys,
        string $resultPrefix, string $buildingPrefix, string $wakePrefix,
        string $hash, string $lockSuffix, string $lockToken
    ): array {
        $result = $this->store->script(
            RedisScripts::get('fetch_versioned_result'),
            array_merge($versionKeys, $scheduledKeys, [$resultPrefix, $buildingPrefix, $wakePrefix]),
            [$hash, $lockSuffix, (string) $this->buildingLockTtl, (string) (int) floor(microtime(true) * 1000), $lockToken, (string) $this->staleVersionDepth]
        );

        return [LuaStatus::fromLua($result[0] ?? null), (string) ($result[1] ?? ''), $result[2] ?? null, $result[3] ?? null];
    }

    private function luaFetchVersionedPivotSegment(array $versionKeys, array $scheduledKeys): string
    {
        $result = $this->store->script(
            RedisScripts::get('fetch_versioned_pivot'),
            array_merge($versionKeys, $scheduledKeys),
            [(string) (int) floor(microtime(true) * 1000)]
        );

        return (string) ($result ?? '');
    }
}
