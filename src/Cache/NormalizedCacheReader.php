<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Values\BuildContext;
use NormCache\Values\QueryCacheResult;

/**
 * Query cache: payload is a bare id list, hydrated from per-model attribute keys.
 *
 * @extends NormalizedReader<QueryCacheResult>
 */
final class NormalizedCacheReader extends NormalizedReader
{
    protected function queryPrefix(string $classKey, ?string $tag): string
    {
        return $this->keys->queryPrefix($classKey, $tag);
    }

    protected function decodePayload(array $result, string $queryKey): array
    {
        $status = LuaStatus::fromLua($result[0] ?? null);

        if (!$status->hasPayload()) {
            return [$status, null, null];
        }

        if (!isset($result[2])) {
            return [LuaStatus::Corrupt, null, null];
        }

        $ids = $this->resolveIdsPayload($result[2], $queryKey);
        if ($ids === null) {
            return [LuaStatus::Corrupt, null, null];
        }

        if (empty($ids)) {
            return [LuaStatus::Empty, [], null];
        }

        return [$status, $ids, null];
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

    protected function buildResult(
        LuaStatus $status,
        BuildContext $build,
        ?array $ids = null,
        ?array $throughKeys = null,
        ?array $models = null,
    ): QueryCacheResult {
        return match ($status) {
            LuaStatus::Hit => new QueryCacheResult(CacheStatus::Hit, $build->queryKey, $ids, $models ?? [], null, null, [], []),
            LuaStatus::Empty => new QueryCacheResult(CacheStatus::Empty, $build->queryKey, [], [], null, null, [], []),
            LuaStatus::Miss => new QueryCacheResult(CacheStatus::Miss, $build->queryKey, null, null, $build->buildingKey, $build->lockToken, $build->versionKeys, $build->expectedVersions, $build->wakeKey),
            LuaStatus::Building => new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []),
            LuaStatus::Corrupt => $this->claimMissAfterCorruptHit($build),
        };
    }

    public function store(string $key, array $ids, ?int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken, ?string $wakeKey = null): bool
    {
        $ids = array_map('strval', $ids);

        return $this->storePayload(
            $key,
            json_encode($ids, JSON_THROW_ON_ERROR),
            $ttl,
            $buildingKey,
            $versionKeys,
            $expectedVersions,
            $buildingToken,
            $wakeKey,
        );
    }
}
