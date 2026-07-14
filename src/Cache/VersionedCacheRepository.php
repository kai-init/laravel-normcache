<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildContext;
use NormCache\Values\BuildHandle;
use NormCache\Values\CacheConfig;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ThroughCacheResult;

/** @template TResult of QueryCacheResult|ThroughCacheResult */
abstract class VersionedCacheRepository
{
    public function __construct(
        protected readonly RedisStore $store,
        protected readonly CacheKeyBuilder $keys,
        protected readonly VersionTracker $versions,
        protected readonly int $queryTtl,
        protected readonly int $buildingLockTtl,
        protected readonly CacheConfig $config,
        protected readonly int $stampedeWaitMs = 200,
    ) {}

    abstract protected function queryPrefix(string $classKey, ?string $tag): string;

    /** @return array{0: LuaStatus, 1: ?array<int|string, mixed>, 2: ?array<int|string, mixed>} */
    abstract protected function decodePayload(array $result, string $queryKey): array;

    /** @return TResult */
    abstract protected function buildResult(
        LuaStatus $status,
        BuildContext $build,
        ?array $ids = null,
        ?array $throughKeys = null,
        ?array $models = null,
    ): QueryCacheResult|ThroughCacheResult;

    /** @return TResult */
    public function fetch(
        string $modelClass,
        string $hash,
        ?string $tag,
        array $depClasses,
        array $depTableKeys,
        ?string $connection = null,
    ): QueryCacheResult|ThroughCacheResult {
        $classKey = $this->keys->classKey($modelClass, $connection);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs(
            $classKey,
            $depClasses,
            $depTableKeys,
            connection: $connection,
        );
        $queryPrefix = $this->queryPrefix($classKey, $tag);
        $lockToken = $this->versions->buildLockToken();

        $result = $this->store->fetchVersionedPayload(
            $versionKeys,
            $scheduledKeys,
            $queryPrefix,
            $this->keys->buildingPrefix($classKey),
            $this->keys->wakePrefix($classKey),
            $hash,
            $hash,
            $lockToken,
            $this->buildingLockTtl,
            $this->config->cooldownEnabled(),
        );

        $seg = (string) ($result[1] ?? '');
        $queryKey = $queryPrefix . $seg . ':' . $hash;
        $buildingKey = $this->keys->resultBuildingKey($classKey, $seg, $hash);
        $wakeKey = $this->keys->wakeKey($classKey, $hash);
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        [$status, $ids, $throughKeys] = $this->decodePayload($result, $queryKey);

        // Use the version Lua already resolved atomically; a separate GET would race with version bumps.
        $modelVersion = is_array($ids) ? (int) ($expectedVersions[0] ?? 0) : 0;
        $models = is_array($ids) ? $this->fetchModels($classKey, $modelVersion, $ids) : null;

        return $this->buildResult(
            $status,
            new BuildContext(
                queryKey: $queryKey,
                buildingKey: $buildingKey,
                lockToken: (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $lockToken),
                wakeKey: $wakeKey,
                versionKeys: $versionKeys,
                expectedVersions: $expectedVersions,
            ),
            $ids,
            $throughKeys,
            $models,
        );
    }

    /** @return TResult|null */
    public function waitForBuild(
        string $modelClass,
        string $hash,
        ?string $tag,
        array $depClasses,
        array $depTableKeys,
        ?string $connection = null,
    ): QueryCacheResult|ThroughCacheResult|null {
        $classKey = $this->keys->classKey($modelClass, $connection);
        $this->store->brpop($this->keys->wakeKey($classKey, $hash), $this->stampedeWaitMs / 1000.0);

        $result = $this->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys, $connection);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    /** @return TResult */
    protected function claimMissAfterCorruptHit(BuildContext $build): QueryCacheResult|ThroughCacheResult
    {
        if ($this->store->setNxEx($build->buildingKey, $build->lockToken, $this->buildingLockTtl)) {
            $this->store->delete($build->wakeKey);

            return $this->buildResult(LuaStatus::Miss, $build);
        }

        return $this->buildResult(LuaStatus::Building, $build);
    }

    protected function storePayload(
        string $key,
        string $payload,
        ?int $ttl,
        BuildHandle $build,
    ): bool {
        return $this->store->storeVersionedPayload(
            [$key => $payload],
            $ttl ?? $this->queryTtl,
            $build->versionKeys,
            $build->expectedVersions,
            $build->buildingKey,
            $build->wakeKey,
            $build->buildingToken,
        );
    }

    private function fetchModels(string $classKey, int $modelVersion, array $ids): array
    {
        if ($ids === []) {
            return [];
        }

        $modelPrefix = $this->keys->modelPrefix($classKey, $modelVersion);

        return $this->store->getMany(array_map(static fn($id) => $modelPrefix . $id, $ids));
    }
}
