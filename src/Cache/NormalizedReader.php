<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildContext;
use NormCache\Values\CacheConfig;
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
        protected readonly CacheConfig $config,
        protected readonly int $stampedeWaitMs = 200,
        protected readonly int $wakeTokenCount = 64,
    ) {}

    // Live runtime toggle; only payload reads honor it — pivot/standalone version reads always check scheduled keys.
    protected function cooldownEnabled(): bool
    {
        return $this->config->cooldown > 0;
    }

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

    /**
     * @return TResult
     */
    public function fetch(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): QueryCacheResult|ThroughCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->queryPrefix($classKey, $tag);
        $lockToken = $this->versions->buildLockToken();
        $cooldown = $this->cooldownEnabled();

        $result = $this->store->script(
            RedisScripts::get('fetch_versioned_payload'),
            array_merge($versionKeys, $cooldown ? $scheduledKeys : [], [
                $queryPrefix,
                $this->keys->buildingPrefix($classKey),
                $this->keys->wakePrefix($classKey),
            ]),
            [
                $hash,
                $hash,
                (int) floor(microtime(true) * 1000),
                $this->buildingLockTtl,
                $lockToken,
                (string) count($versionKeys),
                $cooldown ? '1' : '0',
            ]
        );

        $seg = (string) ($result[1] ?? '');
        $queryKey = $queryPrefix . $seg . ':' . $hash;
        $buildingKey = $this->keys->buildingPrefix($classKey) . $seg . ':' . $hash;
        $wakeKey = $this->keys->wakePrefix($classKey) . $hash;
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
        BuildContext $build,
    ): QueryCacheResult|ThroughCacheResult {
        if ($this->store->setNxEx($build->buildingKey, $build->lockToken, $this->buildingLockTtl)) {
            $this->store->delete($build->wakeKey);

            return $this->buildResult(LuaStatus::Miss, $build);
        }

        return $this->buildResult(LuaStatus::Building, $build);
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

    private function fetchModels(string $classKey, int $modelVersion, array $ids): array
    {
        if ($ids === []) {
            return [];
        }

        $modelPrefix = $this->keys->modelPrefix($classKey, $modelVersion);

        return $this->store->getMany(array_map(static fn($id) => $modelPrefix . $id, $ids));
    }
}
