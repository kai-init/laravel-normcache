<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Collection;
use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Enums\ResultKind;
use NormCache\Payload\PivotIndexAdapter;
use NormCache\Payload\ThroughIndexAdapter;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildHandle;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\VersionedPayloadOutcome;

final class RelationIndexCache
{
    public function __construct(
        private readonly VersionedPayloadStore $payloads,
        private readonly ThroughIndexAdapter $throughAdapter,
        private readonly PivotIndexAdapter $pivotAdapter,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionStore $versions,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly int $stampedeWaitMs,
    ) {}

    public function getOrBuildThrough(
        callable $build,
        string $modelClass,
        string $hash,
        ?string $tag,
        array $depClasses,
        array $depTableKeys,
        ?int $ttl = null,
        ?string $connection = null,
    ): VersionedPayloadOutcome {
        return $this->payloads->getOrBuild(
            adapter: $this->throughAdapter,
            build: $build,
            modelClass: $modelClass,
            hash: $hash,
            tag: $tag,
            depClasses: $depClasses,
            depTableKeys: $depTableKeys,
            kind: CacheKind::RelationIndex,
            ttl: $ttl,
            connection: $connection,
        );
    }

    public function runPivot(
        string $parentClass,
        string $relatedClass,
        string $relation,
        array $parentIds,
        string $constraintHash,
        string $pivotTableKey,
        callable $onBuild,
        callable $onMiss,
        callable $onStore,
        callable $onHit,
        ?string $connection = null,
    ): Collection {
        $result = $this->fetchPivot(
            $parentClass,
            $relatedClass,
            $relation,
            $parentIds,
            $constraintHash,
            $pivotTableKey,
            $connection,
        );

        if ($result->status === CacheStatus::Building) {
            $result = $this->waitForPivotBuild(
                $parentClass,
                $relatedClass,
                $relation,
                $parentIds,
                $constraintHash,
                $pivotTableKey,
                $connection,
            );

            if ($result === null) {
                CacheReporter::metric(
                    'build_budget_exhausted',
                    1,
                    CacheKind::RelationIndex,
                    CacheStatus::Building,
                    $relatedClass,
                    ResultKind::Collection,
                    $this->keys->activeSpace(),
                    ['relation' => $relation, 'parents' => count($parentIds)],
                );

                return $onBuild();
            }
        }

        if ($result->missedIds() === []) {
            return $onHit($result);
        }

        [$models, $cacheModels] = $onMiss($result);
        $onStore($cacheModels, $result);

        return $models;
    }

    public function fetchPivot(
        string $parentClass,
        string $relatedClass,
        string $relation,
        array $parentIds,
        string $constraintHash,
        string $pivotTableKey,
        ?string $connection = null,
    ): PivotCacheResult {
        $measure = CacheReporter::detailed();
        $startedAt = $measure ? microtime(true) : null;
        $parentKey = $this->keys->classKey($parentClass);
        $relatedKey = $this->keys->classKey($relatedClass, $connection);
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs(
            $relatedKey,
            [],
            [$pivotTableKey],
        );
        $segment = $this->store->fetchVersionedPivotSegment($versionKeys, $scheduledKeys);
        $pivotKeys = [];
        foreach ($parentIds as $id) {
            $pivotKeys[] = $this->keys->pivotKey(
                $parentKey,
                $relatedKey,
                $relation,
                $constraintHash,
                $segment,
                $id,
            );
        }

        [$payloads, $corruptCount, $payloadBytes] = $this->decodePivotPayloads(
            $pivotKeys,
            $this->store->getRawMany($pivotKeys),
        );
        $data = array_combine($parentIds, $payloads);
        $expectedVersions = $this->keys->versionsFromSegment($segment);
        $meta = $measure ? [
            'redis_time_ms' => $this->elapsedMs($startedAt),
            'serialized_payload_bytes' => $payloadBytes,
            'index_cardinality' => $this->pivotCardinality($data),
            'corrupt_payload_count' => $corruptCount,
        ] : [];
        $this->reportPivotCorruption($relatedClass, $relation, $corruptCount, $payloadBytes);
        $missed = array_keys(array_filter($data, fn($payload) => !is_array($payload)));

        if ($missed === []) {
            return new PivotCacheResult(
                $segment,
                $data,
                new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions),
                CacheStatus::Hit,
                [...$meta, 'cache_event' => 'hit'],
            );
        }

        [$lockKey, $wakeKey] = $this->pivotLockKeys(
            $relatedKey,
            $relation,
            $constraintHash,
            $parentIds,
            $segment,
        );
        $token = $this->versions->buildLockToken();
        $missedKeys = [];
        foreach ($missed as $id) {
            $missedKeys[] = $this->keys->pivotKey(
                $parentKey,
                $relatedKey,
                $relation,
                $constraintHash,
                $segment,
                $id,
            );
        }

        $retryStarted = $measure ? microtime(true) : null;
        $result = $this->store->fetchBatchBuildStatus(
            $missedKeys,
            $lockKey,
            $wakeKey,
            $token,
            $this->buildingLockTtl,
        );
        [$retryPayloads, $retryCorrupt, $retryBytes] = $this->decodePivotPayloads(
            $missedKeys,
            $result[3] ?? [],
        );
        if ($measure) {
            $meta['redis_time_ms'] += $this->elapsedMs($retryStarted);
            $meta['serialized_payload_bytes'] += $retryBytes;
            $meta['corrupt_payload_count'] += $retryCorrupt;
        }
        $this->reportPivotCorruption($relatedClass, $relation, $retryCorrupt, $retryBytes);
        foreach ($missed as $index => $id) {
            if (isset($retryPayloads[$index]) && is_array($retryPayloads[$index])) {
                $data[$id] = $retryPayloads[$index];
            }
        }
        if ($measure) {
            $meta['index_cardinality'] = $this->pivotCardinality($data);
        }

        if (array_filter($data, fn($payload) => !is_array($payload)) === []) {
            return new PivotCacheResult(
                $segment,
                $data,
                new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions),
                CacheStatus::Hit,
                [...$meta, 'cache_event' => 'hit'],
            );
        }

        if (LuaStatus::fromLua($result[0] ?? null) === LuaStatus::Miss) {
            return new PivotCacheResult(
                $segment,
                $data,
                new BuildHandle($lockKey, $token, $wakeKey, $versionKeys, $expectedVersions),
                CacheStatus::Miss,
                [...$meta, 'cache_event' => 'miss'],
            );
        }

        return new PivotCacheResult(
            $segment,
            $data,
            new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions),
            CacheStatus::Building,
            [...$meta, 'cache_event' => 'building'],
        );
    }

    public function waitForPivotBuild(
        string $parentClass,
        string $relatedClass,
        string $relation,
        array $parentIds,
        string $constraintHash,
        string $pivotTableKey,
        ?string $connection = null,
    ): ?PivotCacheResult {
        $relatedKey = $this->keys->classKey($relatedClass, $connection);
        [, $wakeKey] = $this->pivotLockKeys(
            $relatedKey,
            $relation,
            $constraintHash,
            $parentIds,
            null,
        );
        $startedAt = CacheReporter::detailed() ? microtime(true) : null;
        $this->store->brpop($wakeKey, $this->stampedeWaitMs / 1000.0);
        $waitTimeMs = $this->elapsedMs($startedAt);
        $result = $this->fetchPivot(
            $parentClass,
            $relatedClass,
            $relation,
            $parentIds,
            $constraintHash,
            $pivotTableKey,
            $connection,
        );

        if ($result->status === CacheStatus::Building) {
            return null;
        }

        return new PivotCacheResult(
            $result->seg,
            $result->data,
            $result->build,
            $result->status,
            [
                ...$result->meta,
                'waited' => true,
                'redis_time_ms' => ($result->meta['redis_time_ms'] ?? 0) + $waitTimeMs,
            ],
        );
    }

    public function storePivotEntries(
        array $entries,
        ?int $ttl,
        BuildHandle $build,
        string $modelClass,
    ): bool {
        $encoded = array_map(fn($payload) => $this->pivotAdapter->encode($payload), $entries);
        $startedAt = CacheReporter::detailed() ? microtime(true) : null;
        $stored = $this->store->storeVersionedPayload(
            $encoded,
            $ttl ?? $this->queryTtl,
            $build->versionKeys,
            $build->expectedVersions,
            $build->buildingKey,
            $build->wakeKey,
            $build->buildingToken,
        );

        CacheReporter::metric(
            'pivot_payload_store',
            count($entries),
            CacheKind::RelationIndex,
            CacheStatus::Miss,
            $modelClass,
            ResultKind::Collection,
            $this->keys->activeSpace(),
            [
                'index_cardinality' => array_sum(array_map('count', $entries)),
                'write_committed' => $stored,
                ...($startedAt !== null ? [
                    'redis_time_ms' => $this->elapsedMs($startedAt),
                    'serialized_payload_bytes' => array_sum(array_map('strlen', $encoded)),
                ] : []),
            ],
        );

        return $stored;
    }

    private function decodePivotPayloads(array $keys, array $rawPayloads): array
    {
        $payloads = [];
        $corruptCount = 0;
        $payloadBytes = 0;

        foreach ($keys as $index => $key) {
            $raw = $rawPayloads[$index] ?? null;
            if (!is_string($raw)) {
                $payloads[] = null;

                continue;
            }

            $payloadBytes += strlen($raw);
            $decoded = $this->pivotAdapter->decode($raw);
            if ($decoded->valid) {
                $payloads[] = $decoded->payload;

                continue;
            }

            $corruptCount++;
            $payloads[] = null;
            $this->store->delete($key);
        }

        return [$payloads, $corruptCount, $payloadBytes];
    }

    private function reportPivotCorruption(
        string $relatedClass,
        string $relation,
        int $corruptCount,
        int $payloadBytes,
    ): void {
        if ($corruptCount === 0) {
            return;
        }

        CacheReporter::metric(
            'corrupt_payloads',
            $corruptCount,
            CacheKind::RelationIndex,
            CacheStatus::Miss,
            $relatedClass,
            ResultKind::Collection,
            $this->keys->activeSpace(),
            ['relation' => $relation, 'serialized_payload_bytes' => $payloadBytes],
        );
    }

    private function pivotCardinality(array $data): int
    {
        $count = 0;
        foreach ($data as $entries) {
            if (is_array($entries)) {
                $count += count($entries);
            }
        }

        return $count;
    }

    private function elapsedMs(?float $startedAt): float
    {
        return $startedAt === null ? 0.0 : (microtime(true) - $startedAt) * 1000;
    }

    private function pivotLockKeys(
        string $relatedKey,
        string $relation,
        string $constraintHash,
        array $parentIds,
        ?string $segment,
    ): array {
        $sortedIds = $parentIds;
        sort($sortedIds);
        $lockSuffix = $this->keys->resultBuildIdentityHash(
            'pivot',
            $relation,
            $constraintHash . ':' . implode(',', $sortedIds),
        );

        return [
            $segment !== null
                ? $this->keys->resultBuildingKey($relatedKey, $segment, $lockSuffix)
                : null,
            $this->keys->wakeKey($relatedKey, $lockSuffix),
        ];
    }
}
