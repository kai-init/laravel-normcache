<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildHandle;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheSpace;

final class ModelCacheRepository
{
    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionTracker $versions,
        private readonly CacheConfig $config,
    ) {}

    public function store(string $modelClass, array $modelAttrs, ?CacheSpace $space = null): void
    {
        $space ??= $this->keys->activeSpace();
        $modelVersion = $this->versions->currentVersion($modelClass, $space);

        $this->storeForVersion($modelClass, $modelAttrs, $modelVersion, $space);
    }

    public function storeForBuild(
        string $modelClass,
        array $modelAttrs,
        BuildHandle $build,
        ?CacheSpace $space = null,
    ): void {
        $classKey = $this->keys->classKey($modelClass);
        $index = array_search($this->keys->verKey($classKey, $space), $build->versionKeys, true);

        if ($index === false || !isset($build->expectedVersions[$index])) {
            return;
        }

        $this->storeForVersion($modelClass, $modelAttrs, (int) $build->expectedVersions[$index], $space);
    }

    public function storeForVersion(
        string $modelClass,
        array $modelAttrs,
        int $expectedVersion,
        ?CacheSpace $space = null,
    ): void {
        if (empty($modelAttrs)) {
            return;
        }

        $classKey = $this->keys->classKey($modelClass);
        $attrsByKey = [];

        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey[$this->keys->modelPrefix($classKey, $expectedVersion, $space) . $id] = $attrs;
        }

        $this->store->setManyIfVersion(
            $attrsByKey,
            $this->config->ttl,
            $this->keys->verKey($classKey, $space),
            $expectedVersion,
        );
    }
}
