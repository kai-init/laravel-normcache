<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildHandle;
use NormCache\Values\CacheConfig;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\ResultCacheResult;

final class ResultCacheRepository
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly CacheConfig $config,
        private readonly int $stampedeWaitMs,
    ) {}

    public function fetch(
        string $modelClass, array $depClasses, string $hash,
        ?string $tag, array $depTableKeys,
        string $namespace = CacheKeyBuilder::K_RESULT,
        ?string $connection = null,
    ): ResultCacheResult {
        $classKey = $this->keys->classKey($modelClass, $connection);
        $lockSuffix = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs(
            $classKey,
            $depClasses,
            $depTableKeys,
            connection: $connection,
        );
        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);
        $lockToken = $this->versions->buildLockToken();

        $result = $this->store->fetchVersionedPayload(
            $versionKeys,
            $scheduledKeys,
            $this->keys->namespacedPrefix($namespace, $classKey, $tag),
            $this->keys->buildingPrefix($classKey),
            $this->keys->wakePrefix($classKey),
            $hash,
            $lockSuffix,
            $lockToken,
            $this->buildingLockTtl,
            $this->config->cooldownEnabled(),
        );

        $status = LuaStatus::fromLua($result[0] ?? null);
        $seg = (string) ($result[1] ?? '');
        $resultKey = $this->keys->namespacedKey($namespace, $classKey, $tag, $seg, $hash);
        $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $lockSuffix);
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        return $this->toResultCacheResult(
            $status, $resultKey, $buildingKey, (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $lockToken), $wakeKey,
            $versionKeys, $expectedVersions, $result[2] ?? null, false, $status === LuaStatus::Miss
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
        if ($status === LuaStatus::Hit) {
            $unserialized = $payloadAlreadyUnserialized ? $payload : $this->store->unserialize($payload);
            if (is_array($unserialized)) {
                return new ResultCacheResult(CacheStatus::Hit, $resultKey, $unserialized, new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions));
            }

            $alreadyClaimed = false;
        }

        if ($status === LuaStatus::Building) {
            return new ResultCacheResult(CacheStatus::Building, null, null);
        }

        if ($alreadyClaimed || $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
            if (!$alreadyClaimed) {
                $this->store->delete($wakeKey);
            }

            return new ResultCacheResult(CacheStatus::Miss, $resultKey, null, new BuildHandle($buildingKey, $lockToken, $wakeKey, $versionKeys, $expectedVersions));
        }

        return new ResultCacheResult(CacheStatus::Building, null, null);
    }

    public function fetchPivot(
        string $parentClass, string $relatedClass, string $relation,
        array $parentIds, string $constraintHash, ?string $pivotTableKey
    ): PivotCacheResult {
        $parentKey = $this->keys->classKey($parentClass);
        $relatedKey = $this->keys->classKey($relatedClass);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($relatedKey, [], [$pivotTableKey ?? $parentKey]);

        $seg = $this->store->fetchVersionedPivotSegment($versionKeys, $scheduledKeys);

        $pivotKeys = [];
        foreach ($parentIds as $id) {
            $pivotKeys[] = $this->keys->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id);
        }

        $data = array_combine($parentIds, $this->store->getMany($pivotKeys));
        $expectedVersions = $this->keys->versionsFromSegment($seg);
        $missed = array_keys(array_filter($data, fn($payload) => !is_array($payload)));

        if ($missed === []) {
            return new PivotCacheResult($seg, $data, new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions));
        }

        [$lockKey, $wakeKey] = $this->pivotLockKeys($relatedKey, $relation, $constraintHash, $parentIds, $seg);
        $token = $this->versions->buildLockToken();

        $missedKeys = [];
        foreach ($missed as $id) {
            $missedKeys[] = $this->keys->pivotKey($parentKey, $relatedKey, $relation, $constraintHash, $seg, $id);
        }

        $result = $this->store->fetchBatchBuildStatus($missedKeys, $lockKey, $wakeKey, $token, $this->buildingLockTtl);

        $raw = $this->store->unserializeMany($result[3] ?? []);
        foreach ($missed as $i => $id) {
            if (isset($raw[$i]) && is_array($raw[$i])) {
                $data[$id] = $raw[$i];
            }
        }

        if (array_filter($data, fn($payload) => !is_array($payload)) === []) {
            return new PivotCacheResult($seg, $data, new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions));
        }

        if (LuaStatus::fromLua($result[0] ?? null) === LuaStatus::Miss) {
            return new PivotCacheResult(
                $seg, $data,
                new BuildHandle($lockKey, $token, $wakeKey, $versionKeys, $expectedVersions),
                CacheStatus::Miss
            );
        }

        return new PivotCacheResult(
            $seg, $data,
            new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions),
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
        string $key,
        mixed $payload,
        ?int $ttl,
        BuildHandle $build,
    ): bool {
        $ttl ??= $this->queryTtl;

        if ($build->versionKeys !== []) {
            return $this->storeEntry($key, $payload, $ttl, $build);
        }

        return $this->store->storeSerializedAndRelease(
            $key,
            $payload,
            $ttl,
            $build->buildingKey,
            $build->wakeKey,
            $build->buildingToken,
        );
    }

    public function storeMany(
        array $entries,
        ?int $ttl = null,
        BuildHandle $build = new BuildHandle,
    ): bool {
        if (empty($entries)) {
            return true;
        }

        $ttl ??= $this->queryTtl;

        return $this->store->storeVersionedPayload(
            array_map(fn($p) => $this->store->serialize($p), $entries),
            $ttl,
            $build->versionKeys,
            $build->expectedVersions,
            $build->buildingKey,
            $build->wakeKey,
            $build->buildingToken,
        );
    }

    public function storeEntry(
        string $key,
        mixed $payload,
        ?int $ttl = null,
        BuildHandle $build = new BuildHandle,
    ): bool {
        return $this->storeMany([$key => $payload], $ttl, $build);
    }

    public function waitForBuild(
        string $modelClass, array $depClasses, string $hash,
        ?string $tag, array $depTableKeys,
        string $namespace = CacheKeyBuilder::K_RESULT,
        ?string $connection = null,
    ): ?ResultCacheResult {
        $classKey = $this->keys->classKey($modelClass, $connection);
        $wakeSuffix = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);
        $this->store->brpop($this->keys->wakePrefix($classKey) . $wakeSuffix, $this->stampedeWaitMs / 1000.0);

        $result = $this->fetch($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace, $connection);

        return $result->status === CacheStatus::Building ? null : $result;
    }
}
