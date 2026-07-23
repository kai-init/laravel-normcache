<?php

namespace NormCache\Cache;

use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Enums\ResultKind;
use NormCache\Payload\PayloadAdapter;
use NormCache\Support\CacheKeyBuilder;
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
        $classKey = $this->keys->classKey($modelClass, $connection);
        $namespace = $this->keys->namespaceFor($kind, $resultKind);
        $lockSuffix ??= $hash;
        [$versionKeys, $scheduledKeys] = $this->keys->depKeyPairs(
            $classKey,
            $depClasses,
            $depTableKeys,
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
        );

        if ($fetched->status === CacheStatus::Hit || $fetched->status === CacheStatus::Empty) {
            return $fetched;
        }

        if ($fetched->status === CacheStatus::Building) {
            $this->store->brpop($wakeKey, $this->stampedeWaitMs / 1000.0);
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
            );

            if ($retried->status === CacheStatus::Hit || $retried->status === CacheStatus::Empty) {
                return $retried;
            }

            if ($retried->status === CacheStatus::Building) {
                $payload = $build();

                return new VersionedPayloadOutcome(
                    payload: $payload,
                    status: CacheStatus::Building,
                    key: $retried->key,
                    build: new BuildHandle,
                );
            }

            $fetched = $retried;
        }

        try {
            $payload = $build();
            $encoded = $adapter->encode($payload);
            $this->store->storeVersionedPayload(
                [$fetched->key => $encoded],
                $ttl ?? $this->queryTtl,
                $fetched->build->versionKeys,
                $fetched->build->expectedVersions,
                $fetched->build->buildingKey,
                $fetched->build->wakeKey,
                $fetched->build->buildingToken,
            );
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
    ): VersionedPayloadOutcome {
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
                );
            }

            $this->store->delete($key);
            $claimed = $this->store->setNxEx($buildingKey, $token, $this->buildingLockTtl);
            if ($claimed) {
                $this->store->delete($wakeKey);
            }

            return $claimed
                ? new VersionedPayloadOutcome(null, CacheStatus::Miss, $key, $build)
                : new VersionedPayloadOutcome(null, CacheStatus::Building, $key, new BuildHandle);
        }

        if ($status === LuaStatus::Miss) {
            return new VersionedPayloadOutcome(
                null,
                CacheStatus::Miss,
                $key,
                $build,
            );
        }

        return new VersionedPayloadOutcome(
            null,
            CacheStatus::Building,
            $key,
            new BuildHandle,
        );
    }
}
