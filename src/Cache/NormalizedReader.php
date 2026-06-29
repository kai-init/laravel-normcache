<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ThroughCacheResult;

/**
 * Shared fetch/store/wait flow for the normalized id-list caches. Subclasses
 * supply only the key prefix, payload codec, and result DTO via three hooks.
 *
 * @template TResult of QueryCacheResult|ThroughCacheResult
 */
abstract class NormalizedReader
{
    public function __construct(
        protected readonly RedisStore $store,
        protected readonly CacheKeyBuilder $keys,
        protected readonly VersionTracker $versions,
        protected readonly int $queryTtl,
        protected readonly int $buildingLockTtl,
        protected readonly int $stampedeWaitMs = 200,
        protected readonly int $wakeTokenCount = 64,
    ) {}

    // Cache-key prefix for this reader's namespace (query vs through).
    abstract protected function queryPrefix(string $classKey, ?string $tag): string;

    /**
     * Decode the Lua payload to [status, ids, extra]. ids: list on hit, [] empty,
     * null otherwise. extra: reader-specific (e.g. through keys).
     *
     * @return array{0: LuaStatus, 1: ?array<int|string, mixed>, 2: mixed}
     */
    abstract protected function decodePayload(array $result, string $queryKey): array;

    /**
     * Build the concrete result DTO for this reader.
     *
     * @return TResult
     */
    abstract protected function buildResult(
        LuaStatus $status,
        string $queryKey,
        string $buildingKey,
        string $lockToken,
        string $wakeKey,
        array $versionKeys,
        array $expectedVersions,
        ?array $ids = null,
        mixed $extra = null,
        ?array $models = null,
    ): QueryCacheResult|ThroughCacheResult;

    /**
     * @return TResult
     */
    public function fetch(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): QueryCacheResult|ThroughCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->queryPrefix($classKey, $tag);
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
        $wakeKey = $this->keys->wakePrefix($classKey) . $hash;
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        [$status, $ids, $extra] = $this->decodePayload($result, $queryKey);

        // Use the version Lua already resolved atomically; a separate GET would race with version bumps.
        $modelVersion = is_array($ids) ? (int) ($expectedVersions[0] ?? 0) : 0;
        $models = is_array($ids) ? $this->fetchModels($classKey, $modelVersion, $ids) : null;

        return $this->buildResult(
            $status,
            $queryKey,
            $buildingKey,
            (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $lockToken),
            $wakeKey,
            $versionKeys,
            $expectedVersions,
            $ids,
            $extra,
            $models,
        );
    }

    /**
     * @return TResult|null
     */
    public function waitForBuild(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): QueryCacheResult|ThroughCacheResult|null
    {
        $classKey = $this->keys->classKey($modelClass);
        $this->store->brpop($this->keys->wakePrefix($classKey) . $hash, $this->stampedeWaitMs / 1000.0);

        $result = $this->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    /**
     * Corrupt hit: claim the build lock to rebuild, else report Building.
     *
     * @return TResult
     */
    protected function claimMissAfterCorruptHit(
        string $queryKey,
        string $buildingKey,
        string $lockToken,
        string $wakeKey,
        array $versionKeys,
        array $expectedVersions,
    ): QueryCacheResult|ThroughCacheResult {
        if ($this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)) {
            $this->store->delete($wakeKey);

            return $this->buildResult(LuaStatus::Miss, $queryKey, $buildingKey, $lockToken, $wakeKey, $versionKeys, $expectedVersions);
        }

        return $this->buildResult(LuaStatus::Building, $queryKey, $buildingKey, $lockToken, $wakeKey, $versionKeys, $expectedVersions);
    }

    // Store an encoded payload, version-guarded, releasing the build lock.
    protected function storePayload(
        string $key,
        string $payload,
        ?int $ttl,
        ?string $buildingKey,
        array $versionKeys,
        array $expectedVersions,
        ?string $buildingToken,
        ?string $wakeKey = null,
    ): bool {
        $ttl ??= $this->queryTtl;

        if (!empty($versionKeys)) {
            $keys = array_merge($versionKeys, [$key]);
            if ($buildingKey !== null) {
                $keys[] = $buildingKey;
                if ($wakeKey !== null) {
                    $keys[] = $wakeKey;
                }
            }

            return (bool) $this->store->script(
                RedisScripts::get('store_versioned_payload'),
                $keys,
                array_merge(
                    [(string) count($versionKeys), '1', (string) $ttl],
                    $expectedVersions,
                    [$payload, $buildingToken ?? '', (string) $this->wakeTokenCount]
                )
            );

        }

        if ($buildingKey === null) {
            return false;
        }

        return $this->store->storeRawAndRelease(
            $key,
            $payload,
            $ttl,
            $buildingKey,
            $wakeKey,
            $buildingToken
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
