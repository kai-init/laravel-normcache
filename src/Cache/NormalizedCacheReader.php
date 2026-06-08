<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
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
        private readonly int $staleVersionDepth,
        private readonly int $stampedeWaitMs = 200,
    ) {}

    public function fetch(string $modelClass, string $hash, ?string $tag, array $depClasses, array $depTableKeys): QueryCacheResult
    {
        $classKey = $this->keys->classKey($modelClass);

        if (empty($depClasses) && empty($depTableKeys)) {
            $lockToken = $this->versions->buildLockToken();
            $result = $this->luaFetchVersionedQuery($classKey, $hash, $tag, $lockToken);

            $status = $result[0] ?? 'building';
            $version = $this->versions->normalizeVersion($result[1]);
            $queryKey = $this->keys->queryKey($classKey, $tag, $version, $hash);
            $buildingKey = $this->keys->buildingPrefix($classKey) . $hash;

            return $this->toQueryResult(
                $status, $queryKey, $buildingKey, (string) (($status === 'miss' ? $result[2] : null) ?? $lockToken),
                [$this->keys->verKey($classKey)],
                [(string) $version],
                in_array($status, ['hit', 'stale']) ? $result[2] : null,
                in_array($status, ['hit', 'stale']) ? $result[3] : null
            );
        }

        $versionKeys = $this->keys->depVersionKeys($classKey, $depClasses, $depTableKeys);
        $scheduledKeys = $this->keys->depScheduledKeys($classKey, $depClasses, $depTableKeys);
        $queryPrefix = $this->keys->queryPrefix($classKey, $tag);
        $lockToken = $this->versions->buildLockToken();
        $result = $this->luaFetchMultiVersionedQuery(
            $versionKeys, $scheduledKeys, $queryPrefix,
            $this->keys->modelPrefix($classKey),
            $this->keys->buildingPrefix($classKey),
            $hash, $lockToken
        );

        $status = $result[0] ?? 'building';
        $seg = (string) ($result[1] ?? '');
        $queryKey = $queryPrefix . $seg . ':' . $hash;
        $buildingKey = $this->keys->buildingPrefix($classKey) . $seg . ':' . $hash;
        $expectedVersions = $this->keys->versionsFromSegment($seg);

        return $this->toQueryResult(
            $status, $queryKey, $buildingKey, (string) (($status === 'miss' ? $result[2] : null) ?? $lockToken),
            $versionKeys, $expectedVersions,
            in_array($status, ['hit', 'stale']) ? $result[2] : null,
            in_array($status, ['hit', 'stale']) ? $result[3] : null
        );
    }

    private function toQueryResult(
        string $status,
        string $queryKey,
        string $buildingKey,
        string $lockToken,
        array $versionKeys,
        array $expectedVersions,
        mixed $ids = null,
        mixed $models = null
    ): QueryCacheResult {
        $deserialize = fn($r) => is_array($r) ? $this->store->unserializeMany($r) : [];

        return match ($status) {
            'hit' => new QueryCacheResult(CacheStatus::Hit, $queryKey, $ids, $deserialize($models), null, null, [], []),
            'stale' => new QueryCacheResult(CacheStatus::Stale, null, $ids, $deserialize($models), null, null, [], []),
            'empty' => new QueryCacheResult(CacheStatus::Empty, $queryKey, [], [], null, null, [], []),
            'miss' => new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions),
            'building' => new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
            'corrupt' => $this->store->setNxEx($buildingKey, $lockToken, $this->buildingLockTtl)
                ? new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, $buildingKey, $lockToken, $versionKeys, $expectedVersions)
                : new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
            default => new QueryCacheResult(CacheStatus::Miss, $queryKey, null, null, null, null, [], []),
        };
    }

    public function store(string $key, array $ids, ?int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken): void
    {
        $ids = array_map('strval', $ids);
        $ttl ??= $this->queryTtl;

        if (!empty($versionKeys)) {
            $this->store->eval(
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
        $this->store->brpop(
            $this->keys->wakePrefix($classKey) . $hash,
            $this->stampedeWaitMs / 1000.0
        );
        $result = $this->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);

        return $result->status === CacheStatus::Building ? null : $result;
    }

    private function luaFetchVersionedQuery(string $classKey, string $hash, ?string $tag, string $lockToken): mixed
    {
        return $this->store->eval(RedisScripts::get('fetch_versioned_query'), [
            $this->keys->verKey($classKey),
            $this->keys->scheduledKey($classKey),
            $this->keys->queryPrefix($classKey, $tag),
            $this->keys->modelPrefix($classKey),
            $this->keys->buildingPrefix($classKey),
        ], [$hash, (int) floor(microtime(true) * 1000), $this->buildingLockTtl, $this->staleVersionDepth, $lockToken]);
    }

    private function luaFetchMultiVersionedQuery(
        array $versionKeys, array $scheduledKeys,
        string $queryPrefix, string $modelPrefix, string $buildingPrefix,
        string $hash, string $lockToken,
    ): mixed {
        return $this->store->eval(
            RedisScripts::get('fetch_multi_versioned_query'),
            array_merge($versionKeys, $scheduledKeys, [$queryPrefix, $modelPrefix, $buildingPrefix]),
            [$hash, (int) floor(microtime(true) * 1000), $this->buildingLockTtl, $lockToken]
        );
    }
}
