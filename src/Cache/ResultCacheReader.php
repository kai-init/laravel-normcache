<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
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
    ) {}

    public function fetch(
        string $modelClass, array $depClasses, string $hash,
        ?string $tag, array $depTableKeys,
        string $namespace = CacheKeyBuilder::K_RESULT
    ): ResultCacheResult {
        $classKey = $this->keys->classKey($modelClass);
        $lockSuffix = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);
        $versionKeys = $this->keys->depVersionKeys($classKey, $depClasses, $depTableKeys);
        $scheduledKeys = $this->keys->depScheduledKeys($classKey, $depClasses, $depTableKeys);
        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);

        if ($this->slotting) {
            $resolvedVersions = $this->versions->resolveVersions($versionKeys, $scheduledKeys);
            $seg = $this->keys->versionSegment($versionKeys, $resolvedVersions);
            $expectedVersions = $this->versions->expectedVersions($versionKeys, $resolvedVersions);
            $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
            $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);
            $lockToken = $this->versions->buildLockToken();

            $payload = $this->store->get($resultKey);
            if ($payload !== null) {
                if (is_array($payload)) {
                    return new ResultCacheResult(CacheStatus::Hit, $resultKey, $payload, null, null, null, $versionKeys, $expectedVersions);
                }
                // Corrupt payload (not an array), treat as miss and attempt to claim building lock.
            }

            if (!$this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
                return new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
            }

            return new ResultCacheResult(CacheStatus::Miss, $resultKey, null, $buildingKey, $lockToken, $wakeKey, $versionKeys, $expectedVersions);
        }

        $lockToken = $this->versions->buildLockToken();
        [$status, $seg, $payload, $claimedToken] = $this->luaFetchVersionedResult(
            $versionKeys, $scheduledKeys,
            $this->keys->namespacedPrefix($namespace, $classKey, $tag),
            $this->keys->buildingPrefix($classKey),
            $hash, $lockSuffix, $lockToken
        );

        $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        return match ($status) {
            'hit' => (function () use ($payload, $resultKey, $versionKeys, $expectedVersions, $classKey, $seg, $lockSuffix, $lockToken, $wakeKey) {
                $unserialized = $this->store->unserialize($payload);
                if (is_array($unserialized)) {
                    return new ResultCacheResult(CacheStatus::Hit, $resultKey, $unserialized, null, null, null, $versionKeys, $expectedVersions);
                }

                // Corrupt payload (not an array), treat as miss.
                // We must attempt to claim the building lock to "allow rebuild" (and storage).
                $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);
                if ($this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
                    return new ResultCacheResult(CacheStatus::Miss, $resultKey, null, $buildingKey, (string) $lockToken, $wakeKey, $versionKeys, $expectedVersions);
                }

                return new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
            })(),
            'building' => new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
            default => new ResultCacheResult(CacheStatus::Miss, $resultKey, null, $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix), (string) ($claimedToken ?? $lockToken), $wakeKey, $versionKeys, $expectedVersions),
        };
    }

    public function fetchPivot(
        string $parentClass, string $relatedClass, string $relation,
        array $parentIds, string $constraintHash, ?string $pivotTableKey
    ): PivotCacheResult {
        $parentKey = $this->keys->classKey($parentClass);
        $relatedKey = $this->keys->classKey($relatedClass);
        $versionKeys = $this->keys->depVersionKeys($relatedKey, [], [$pivotTableKey ?? $parentKey]);
        $scheduledKeys = $this->keys->depScheduledKeys($relatedKey, [], [$pivotTableKey ?? $parentKey]);

        if ($this->slotting) {
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

        [$seg, $payloads] = $this->luaFetchVersionedPivotCache(
            $parentKey, $relatedKey, $relation, $constraintHash, $parentIds, $versionKeys, $scheduledKeys
        );

        return new PivotCacheResult(
            (string) $seg,
            array_combine($parentIds, $this->store->unserializeMany($payloads)),
            $versionKeys,
            $this->keys->versionsFromSegment($seg),
        );
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

    public function storeEntry(
        string $key, mixed $payload, int $ttl,
        array $versionKeys, array $expectedVersions,
        ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null
    ): bool {
        if ($this->slotting) {
            if ($buildingKey !== null && $buildingToken !== null && $this->store->getRaw($buildingKey) !== $buildingToken) {
                return false;
            }
            $written = $this->versions->versionsStillMatch($versionKeys, $expectedVersions);
            if ($written) {
                $this->store->set($key, $payload, $ttl);
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

        return (bool) $this->store->eval(
            RedisScripts::get('store_if_versions_match_and_release'),
            array_merge($versionKeys, [
                $key,
                $buildingKey ?? '',
                $wakeKey ?? ($buildingKey !== null ? $this->keys->buildingToWakeKey($buildingKey) : ''),
            ]),
            array_merge(
                [(string) count($versionKeys), (string) $ttl],
                $expectedVersions,
                [$this->store->serialize($payload), $buildingToken ?? '']
            )
        );
    }

    public function waitForBuild(
        string $modelClass, array $depClasses, string $hash,
        ?string $tag, array $depTableKeys,
        string $namespace = CacheKeyBuilder::K_RESULT
    ): ?ResultCacheResult {
        $classKey = $this->keys->classKey($modelClass);
        $wakeHash = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);
        $this->store->brpop(
            $this->keys->wakePrefix($classKey) . $wakeHash,
            $this->stampedeWaitMs / 1000.0
        );
        $result = $this->fetch($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    private function luaFetchVersionedResult(
        array $versionKeys, array $scheduledKeys,
        string $resultPrefix, string $buildingPrefix,
        string $hash, string $lockSuffix, string $lockToken
    ): array {
        $result = $this->store->eval(
            RedisScripts::get('fetch_versioned_result'),
            array_merge($versionKeys, $scheduledKeys, [$resultPrefix, $buildingPrefix]),
            [$hash, $lockSuffix, (string) $this->buildingLockTtl, (string) (int) floor(microtime(true) * 1000), $lockToken]
        );

        return [$result[0] ?? 'building', (string) ($result[1] ?? ''), $result[2] ?? null, $result[3] ?? null];
    }

    private function luaFetchVersionedPivotCache(
        string $parentKey, string $relatedKey, string $relation,
        string $constraintHash, array $parentIds,
        array $versionKeys, array $scheduledKeys
    ): array {
        $result = $this->store->eval(
            RedisScripts::get('fetch_versioned_pivot'),
            array_merge($versionKeys, $scheduledKeys, [$this->keys->pivotBasePrefix($parentKey, $relatedKey)]),
            array_merge([$relation, $constraintHash, (string) (int) floor(microtime(true) * 1000)], $parentIds)
        );

        return [(string) ($result[0] ?? ''), $result[1] ?? []];
    }
}
