<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Values\BuildContext;
use NormCache\Values\ThroughCacheResult;

/**
 * Through cache (HasManyThrough/HasOneThrough): payload is `{i, t}` — id list
 * plus per-row through keys Eloquent needs to re-stitch the relation.
 *
 * @extends NormalizedReader<ThroughCacheResult>
 */
final class NormalizedThroughReader extends NormalizedReader
{
    protected function queryPrefix(string $classKey, ?string $tag): string
    {
        return $this->keys->namespacedPrefix(CacheKeyBuilder::K_THROUGH, $classKey, $tag);
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

        $parsed = json_decode($result[2], true);

        if (!is_array($parsed) || !is_array($parsed['i'] ?? null) || !is_array($parsed['t'] ?? null)) {
            $this->store->delete($queryKey);

            return [LuaStatus::Corrupt, null, null];
        }

        if (empty($parsed['i'])) {
            return [LuaStatus::Empty, [], []];
        }

        return [LuaStatus::Hit, $parsed['i'], $parsed['t']];
    }

    protected function buildResult(
        LuaStatus $status,
        BuildContext $build,
        ?array $ids = null,
        ?array $throughKeys = null,
        ?array $models = null,
    ): ThroughCacheResult {
        return match ($status) {
            LuaStatus::Hit => new ThroughCacheResult(CacheStatus::Hit, $build->queryKey, $ids, $throughKeys, $models ?? []),
            LuaStatus::Empty => new ThroughCacheResult(CacheStatus::Empty, $build->queryKey, [], [], []),
            LuaStatus::Miss => new ThroughCacheResult(CacheStatus::Miss, $build->queryKey, null, null, null, $build->handle()),
            LuaStatus::Building => new ThroughCacheResult(CacheStatus::Building, null, null, null, null),
            LuaStatus::Corrupt => $this->claimMissAfterCorruptHit($build),
        };
    }

    public function store(string $key, array $ids, array $throughKeys, ?int $ttl, ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken, ?string $wakeKey = null): bool
    {
        $ids = array_map('strval', $ids);
        $payload = json_encode(['i' => $ids, 't' => $throughKeys], JSON_THROW_ON_ERROR);

        return $this->storePayload(
            $key,
            $payload,
            $ttl,
            $buildingKey,
            $versionKeys,
            $expectedVersions,
            $buildingToken,
            $wakeKey,
        );
    }
}
