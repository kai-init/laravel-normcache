<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Values\BuildContext;
use NormCache\Values\BuildHandle;
use NormCache\Values\QueryCacheResult;

/** @extends VersionedCacheRepository<QueryCacheResult> */
final class NormalizedCacheRepository extends VersionedCacheRepository
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
            LuaStatus::Hit => new QueryCacheResult(CacheStatus::Hit, $build->queryKey, $ids, $models ?? []),
            LuaStatus::Empty => new QueryCacheResult(CacheStatus::Empty, $build->queryKey, [], []),
            LuaStatus::Miss => new QueryCacheResult(CacheStatus::Miss, $build->queryKey, null, null, $build->handle()),
            LuaStatus::Building => new QueryCacheResult(CacheStatus::Building, null, null, null),
            LuaStatus::Corrupt => $this->claimMissAfterCorruptHit($build),
        };
    }

    public function store(
        string $key,
        array $ids,
        ?int $ttl,
        BuildHandle $build,
    ): bool {
        $ids = array_map('strval', $ids);

        return $this->storePayload(
            $key,
            json_encode($ids, JSON_THROW_ON_ERROR),
            $ttl,
            $build,
        );
    }
}
