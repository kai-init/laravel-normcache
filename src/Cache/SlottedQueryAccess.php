<?php

namespace NormCache\Cache;

/** Shared slotting-safe key resolution and guarded write, used by readers whose keys are rooted at a single classKey. */
trait SlottedQueryAccess
{
    private function usesSlotting(): bool
    {
        return $this->slotting || $this->store->isCluster();
    }

    private function resolveSlotKeys(
        string $classKey, string $hash, string $queryPrefix,
        array $versionKeys, array $scheduledKeys
    ): array {
        $resolvedVersions = $this->versions->resolveVersions($versionKeys, $scheduledKeys);
        $seg = $this->keys->versionSegment($versionKeys, $resolvedVersions);
        $expectedVersions = $this->versions->expectedVersions($versionKeys, $resolvedVersions);

        return [
            $queryPrefix . $seg . ':' . $hash,
            $this->keys->buildingPrefix($classKey) . $seg . ':' . $hash,
            $expectedVersions,
        ];
    }

    private function fetchModels(string $classKey, array $ids): array
    {
        if ($ids === []) {
            return [];
        }

        $modelPrefix = $this->keys->modelPrefix($classKey);

        return $this->store->getMany(array_map(static fn($id) => $modelPrefix . $id, $ids));
    }

    private function storeSlottingGuarded(
        string $key, string $payload, int $ttl,
        ?string $buildingKey, array $versionKeys, array $expectedVersions, ?string $buildingToken
    ): void {
        if ($buildingKey !== null && $buildingToken !== null && $this->store->getRaw($buildingKey) !== $buildingToken) {
            return;
        }

        if ($this->versions->versionsStillMatch($versionKeys, $expectedVersions)) {
            $this->store->setRaw($key, $payload, $ttl);
        }

        if ($buildingKey !== null) {
            $this->store->releaseBuilding($buildingKey, $this->keys->buildingToWakeKey($buildingKey), $buildingToken);
        }
    }
}
