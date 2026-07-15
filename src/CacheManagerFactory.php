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
use NormCache\Values\CacheConfig;

final class CacheManagerFactory
{
    public function __construct(
        private readonly CacheSpaceRegistry $spaceRegistry,
        private readonly CacheSpaceResolver $spaceResolver,
    ) {}

    /** @param  array<string, mixed>  $overrides */
    public function make(array $overrides = []): CacheManager
    {
        $connection = (string) $this->value($overrides, 'connection', 'normcache.connection');
        $ttl = (int) $this->value($overrides, 'ttl', 'normcache.ttl');
        $queryTtl = (int) $this->value($overrides, 'query_ttl', 'normcache.query_ttl');
        $keyPrefix = (string) $this->value($overrides, 'key_prefix', 'normcache.key_prefix', '');
        $cooldown = (int) $this->value($overrides, 'cooldown', 'normcache.cooldown', 0);
        $enabled = (bool) $this->value($overrides, 'enabled', 'normcache.enabled', true);
        $events = (bool) $this->value($overrides, 'events', 'normcache.events', false);
        $fallback = (bool) $this->value($overrides, 'fallback', 'normcache.fallback', true);
        $fireRetrieved = (bool) $this->value($overrides, 'fire_retrieved', 'normcache.fire_retrieved', false);
        $buildingLockTtl = (int) $this->value($overrides, 'building_lock_ttl', 'normcache.building_lock_ttl', 5);
        $stampedeWaitMs = (int) $this->value($overrides, 'stampede_wait_ms', 'normcache.stampede_wait_ms', 200);
        $stampedeWakeTokens = (int) $this->value($overrides, 'stampede_wake_tokens', 'normcache.stampede_wake_tokens', 64);

        $keys = new CacheKeyBuilder('{nc}:', $keyPrefix);
        $store = new RedisStore($connection, $stampedeWakeTokens);
        $versions = new VersionTracker($store, $keys);
        $engine = new ExecutionEngine;
        $config = new CacheConfig(
            ttl: $ttl,
            queryTtl: $queryTtl,
            cooldown: $cooldown,
            enabled: $enabled,
            fallbackEnabled: $fallback,
            dispatchEvents: $events,
            stampedeWakeTokens: $stampedeWakeTokens,
        );
        $queries = new NormalizedCacheRepository($store, $keys, $versions, $queryTtl, $buildingLockTtl, $config, $stampedeWaitMs);
        $results = new ResultCacheRepository($store, $keys, $versions, $queryTtl, $buildingLockTtl, $config, $stampedeWaitMs);
        $through = new ThroughCacheRepository($store, $keys, $versions, $queryTtl, $buildingLockTtl, $config, $stampedeWaitMs);
        $models = new ModelCacheRepository($store, $keys, $versions, $config);

        return new CacheManager(
            queries: $queries,
            results: $results,
            through: $through,
            models: $models,
            result: new ResultExecutor($engine, $results, $config),
            hydrator: new ModelHydrator($store, $keys, $versions, $ttl, $fireRetrieved, $buildingLockTtl, $stampedeWaitMs),
            versions: $versions,
            engine: $engine,
            store: $store,
            keys: $keys,
            config: $config,
            spaceResolver: $this->spaceResolver,
            spaceRegistry: $this->spaceRegistry,
        );
    }

    /** @param  array<string, mixed>  $overrides */
    private function value(array $overrides, string $override, string $configKey, mixed $default = null): mixed
    {
        return array_key_exists($override, $overrides)
            ? $overrides[$override]
            : config($configKey, $default);
    }
}
