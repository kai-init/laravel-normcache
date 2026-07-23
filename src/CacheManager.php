<?php

namespace NormCache;

use NormCache\Cache\Invalidator;
use NormCache\Cache\ModelCache;
use NormCache\Cache\ModelIndexCache;
use NormCache\Cache\RelationIndexCache;
use NormCache\Cache\ResultCache;
use NormCache\Cache\VersionStore;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Traits\HandlesInvalidation;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheSpace;

class CacheManager
{
    use HandlesInvalidation;

    public function __construct(
        private readonly ModelIndexCache $modelIndexes,
        private readonly ResultCache $resultCache,
        private readonly RelationIndexCache $relationIndexes,
        private readonly ModelCache $modelCache,
        private readonly VersionStore $versions,
        private readonly Invalidator $invalidation,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly CacheConfig $config,
        private readonly CacheSpaceResolver $spaceResolver,
    ) {}

    public function modelIndexes(): ModelIndexCache
    {
        return $this->modelIndexes;
    }

    public function resultCache(): ResultCache
    {
        return $this->resultCache;
    }

    public function relationIndexes(): RelationIndexCache
    {
        return $this->relationIndexes;
    }

    public function modelCache(): ModelCache
    {
        return $this->modelCache;
    }

    public function versionStore(): VersionStore
    {
        return $this->versions;
    }

    public function invalidator(): Invalidator
    {
        return $this->invalidation;
    }

    public function config(): CacheConfig
    {
        return $this->config;
    }

    public function isEnabled(): bool
    {
        return $this->config->enabled;
    }

    public function isFallbackEnabled(): bool
    {
        return $this->config->fallbackEnabled;
    }

    public function isEventsEnabled(): bool
    {
        return $this->config->dispatchEvents;
    }

    public function enable(): void
    {
        $this->config->enabled = true;
    }

    public function disable(): void
    {
        $this->config->enabled = false;
    }

    public function store(): RedisStore
    {
        return $this->store;
    }

    public function keys(): CacheKeyBuilder
    {
        return $this->keys;
    }

    public function spaceFor(string $modelClass, ?string $explicitSpace = null): CacheSpace
    {
        return $this->spaceResolver->resolve($modelClass, $explicitSpace);
    }

    public function withSpace(?CacheSpace $space, callable $callback): mixed
    {
        if ($space === null) {
            return $this->keys->withSpace(null, $callback);
        }

        $active = $this->keys->activeSpace();

        return $active !== null && $active->name === $space->name
            ? $callback()
            : $this->keys->withSpace($space, $callback);
    }

    public function withSpaceForModel(string $modelClass, ?string $explicitSpace, callable $callback): mixed
    {
        return $this->withSpace($this->spaceFor($modelClass, $explicitSpace), $callback);
    }

    public function currentVersion(string $modelClass, ?string $connection = null): int
    {
        return $this->versions->currentVersion($modelClass, $this->modelSpaces($modelClass)[0], $connection);
    }

    public function currentTableVersion(string $connectionName, string $table): int
    {
        return $this->versions->currentTableVersion($connectionName, $table);
    }
}
