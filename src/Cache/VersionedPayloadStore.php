<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Enums\ResultKind;
use NormCache\Payload\PayloadAdapter;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildHandle;
use NormCache\Values\CacheConfig;
use NormCache\Values\VersionedPayloadOutcome;

final class VersionedPayloadStore
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionStore $versions,
        private readonly CacheConfig $config,
        private readonly int $queryTtl,
        private readonly int $buildingLockTtl,
        private readonly int $stampedeWaitMs,
    ) {}

    public function getOrBuild(
        PayloadAdapter $adapter,
        callable $build,
        string $modelClass,
        string $hash,
        ?string $tag,
        array $depClasses,
        array $depTableKeys,
        CacheKind $kind,
        ?ResultKind $resultKind = null,
        ?int $ttl = null,
        ?string $connection = null,
        ?string $lockSuffix = null,
    ): VersionedPayloadOutcome {
        $measure = CacheReporter::detailed();
        $classKey = $this->keys->classKey($modelClass, $connection);
        $namespace = $this->keys->namespaceFor($kind, $resultKind);
        $lockSuffix ??= $hash;
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs(
            $classKey,
            $depClasses,
            $depTableKeys,
            connection: $connection,
        );
        $prefix = $this->keys->namespacedPrefix($namespace, $classKey, $tag);
        $wakeKey = $this->keys->wakeKey($classKey, $lockSuffix);
        $token = $this->versions->buildLockToken();

        $fetched = $this->fetch(
            $adapter,
            $versionKeys,
            $scheduledKeys,
            $prefix,
            $classKey,
            $hash,
            $lockSuffix,
            $wakeKey,
            $token,
            $measure,
        );

        if ($fetched->status === CacheStatus::Hit || $fetched->status === CacheStatus::Empty) {
            return $fetched;
        }

        if ($fetched->status === CacheStatus::Building) {
            $waitStarted = $measure ? microtime(true) : null;
            $this->store->brpop($wakeKey, $this->stampedeWaitMs / 1000.0);
            $waitMeta = $measure ? [
                'waited' => true,
                'redis_time_ms' => $this->elapsedMs($waitStarted),
            ] : [];
            $retried = $this->fetch(
                $adapter,
                $versionKeys,
                $scheduledKeys,
                $prefix,
                $classKey,
                $hash,
                $lockSuffix,
                $wakeKey,
                $token,
                $measure,
            );
            $retried = new VersionedPayloadOutcome(
                $retried->payload,
                $retried->status,
                $retried->key,
                $retried->build,
                $this->mergeMeta($fetched->meta, $waitMeta, $retried->meta),
            );

            if ($retried->status === CacheStatus::Hit || $retried->status === CacheStatus::Empty) {
                return $retried;
            }

            if ($retried->status === CacheStatus::Building) {
                $buildStarted = $measure ? microtime(true) : null;
                $payload = $build();

                return new VersionedPayloadOutcome(
                    payload: $payload,
                    status: CacheStatus::Building,
                    key: $retried->key,
                    build: new BuildHandle,
                    meta: $this->mergeMeta($retried->meta, $measure ? [
                        'cache_event' => 'build_budget_exhausted',
                        'build_budget_exhausted' => true,
                        'built' => true,
                        'query_time_ms' => $this->elapsedMs($buildStarted),
                        'index_cardinality' => $this->cardinality($payload),
                    ] : []),
                );
            }

            $fetched = $retried;
        }

        try {
            $buildStarted = $measure ? microtime(true) : null;
            $payload = $build();
            $encoded = $adapter->encode($payload);
            $buildMeta = $measure ? [
                'cache_event' => 'build',
                'built' => true,
                'query_time_ms' => $this->elapsedMs($buildStarted),
                'index_cardinality' => $this->cardinality($payload),
                'serialized_payload_bytes' => strlen($encoded),
            ] : [];
            $storeStarted = $measure ? microtime(true) : null;
            $committed = $this->store->storeVersionedPayload(
                [$fetched->key => $encoded],
                $ttl ?? $this->queryTtl,
                $fetched->build->versionKeys,
                $fetched->build->expectedVersions,
                $fetched->build->buildingKey,
                $fetched->build->wakeKey,
                $fetched->build->buildingToken,
            );
            if ($measure) {
                $buildMeta['redis_time_ms'] = $this->elapsedMs($storeStarted);
                $buildMeta['write_committed'] = $committed;
            }
        } catch (\Throwable $e) {
            $this->store->releaseBuilding(
                $fetched->build->buildingKey ?? '',
                $fetched->build->wakeKey ?? '',
                $fetched->build->buildingToken,
            );

            throw $e;
        }

        return new VersionedPayloadOutcome(
            payload: $payload,
            status: CacheStatus::Miss,
            key: $fetched->key,
            build: $fetched->build,
            meta: $this->mergeMeta($fetched->meta, $buildMeta),
        );
    }

    public function delete(string $key): void
    {
        $this->store->delete($key);
    }

    private function fetch(
        PayloadAdapter $adapter,
        array $versionKeys,
        array $scheduledKeys,
        string $prefix,
        string $classKey,
        string $hash,
        string $lockSuffix,
        string $wakeKey,
        string $token,
        bool $measure,
    ): VersionedPayloadOutcome {
        $startedAt = $measure ? microtime(true) : null;
        $result = $this->store->fetchVersionedPayload(
            $versionKeys,
            $scheduledKeys,
            $prefix,
            $this->keys->buildingPrefix($classKey),
            $this->keys->wakePrefix($classKey),
            $hash,
            $lockSuffix,
            $token,
            $this->buildingLockTtl,
            $this->config->cooldownEnabled(),
        );
        $redisMeta = $measure ? ['redis_time_ms' => $this->elapsedMs($startedAt)] : [];
        $status = LuaStatus::fromLua($result[0] ?? null);
        $segment = (string) ($result[1] ?? '');
        $key = $prefix . $segment . ':' . $hash;
        $buildingKey = $this->keys->resultBuildingKey($classKey, $segment, $lockSuffix);
        $expectedVersions = $this->keys->versionsFromSegment($segment);
        $build = new BuildHandle(
            $buildingKey,
            (string) (($status === LuaStatus::Miss ? $result[2] : null) ?? $token),
            $wakeKey,
            $versionKeys,
            $expectedVersions,
        );

        if ($status === LuaStatus::Hit) {
            $raw = $result[2] ?? null;
            $decoded = $adapter->decode($raw);
            $decodeStatus = $decoded->valid
                ? ($decoded->empty ? LuaStatus::Empty : LuaStatus::Hit)
                : LuaStatus::Corrupt;

            if ($decodeStatus !== LuaStatus::Corrupt) {
                return new VersionedPayloadOutcome(
                    $decoded->payload,
                    $decodeStatus === LuaStatus::Empty ? CacheStatus::Empty : CacheStatus::Hit,
                    $key,
                    new BuildHandle(versionKeys: $versionKeys, expectedVersions: $expectedVersions),
                    $measure ? [
                        ...$redisMeta,
                        'cache_event' => $decodeStatus->value,
                        'index_cardinality' => $this->cardinality($decoded->payload),
                        'serialized_payload_bytes' => is_string($raw) ? strlen($raw) : null,
                    ] : [],
                );
            }

            $deleteStarted = $measure ? microtime(true) : null;
            $this->store->delete($key);
            $claimed = $this->store->setNxEx($buildingKey, $token, $this->buildingLockTtl);
            if ($claimed) {
                $this->store->delete($wakeKey);
            }
            $corruptMeta = $measure ? [
                ...$redisMeta,
                'redis_time_ms' => ($redisMeta['redis_time_ms'] ?? 0) + $this->elapsedMs($deleteStarted),
                'cache_event' => 'corrupt',
                'corrupt_payload' => true,
                'serialized_payload_bytes' => is_string($raw) ? strlen($raw) : null,
            ] : [];

            return $claimed
                ? new VersionedPayloadOutcome(null, CacheStatus::Miss, $key, $build, $corruptMeta)
                : new VersionedPayloadOutcome(null, CacheStatus::Building, $key, new BuildHandle, $corruptMeta);
        }

        if ($status === LuaStatus::Miss) {
            return new VersionedPayloadOutcome(
                null,
                CacheStatus::Miss,
                $key,
                $build,
                $measure ? [...$redisMeta, 'cache_event' => 'miss'] : [],
            );
        }

        return new VersionedPayloadOutcome(
            null,
            CacheStatus::Building,
            $key,
            new BuildHandle,
            $measure ? [...$redisMeta, 'cache_event' => 'building'] : [],
        );
    }

    private function cardinality(mixed $payload): ?int
    {
        return is_countable($payload) ? count($payload) : null;
    }

    private function elapsedMs(?float $startedAt): float
    {
        return $startedAt === null ? 0.0 : (microtime(true) - $startedAt) * 1000;
    }

    private function mergeMeta(array ...$groups): array
    {
        $merged = [];
        foreach ($groups as $group) {
            foreach ($group as $key => $value) {
                if (in_array($key, ['redis_time_ms', 'query_time_ms'], true)) {
                    $merged[$key] = ($merged[$key] ?? 0) + $value;
                } elseif ($value !== null) {
                    $merged[$key] = $value;
                }
            }
        }

        return $merged;
    }
}
