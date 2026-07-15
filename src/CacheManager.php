<?php

namespace NormCache;

use NormCache\Cache\ExecutionEngine;
use NormCache\Cache\ModelCacheRepository;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\NormalizedCacheRepository;
use NormCache\Cache\ResultCacheRepository;
use NormCache\Cache\ResultExecutor;
use NormCache\Cache\ThroughCacheRepository;
use NormCache\Cache\VersionTracker;
use NormCache\Spaces\CacheSpaceRegistry;
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
        private readonly NormalizedCacheRepository $queries,
        private readonly ResultCacheRepository $results,
        private readonly ThroughCacheRepository $through,
        private readonly ModelCacheRepository $models,
        private readonly ResultExecutor $result,
        private readonly ModelHydrator $hydrator,
        private readonly VersionTracker $versions,
        private readonly ExecutionEngine $engine,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly CacheConfig $config,
        private readonly CacheSpaceResolver $spaceResolver,
        private readonly CacheSpaceRegistry $spaceRegistry,
    ) {}

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    public function engine(): ExecutionEngine
    {
        return $this->engine;
    }

    public function result(): ResultExecutor
    {
        return $this->result;
    }

    public function queries(): NormalizedCacheRepository
    {
        return $this->queries;
    }

    public function results(): ResultCacheRepository
    {
        return $this->results;
    }

    public function through(): ThroughCacheRepository
    {
        return $this->through;
    }

    public function models(): ModelCacheRepository
    {
        return $this->models;
    }

    public function config(): CacheConfig
    {
        return $this->config;
    }

    public function hydrator(): ModelHydrator
    {
        return $this->hydrator;
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

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

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
