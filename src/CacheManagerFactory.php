<?php

namespace NormCache;

use NormCache\Cache\Invalidator;
use NormCache\Cache\ModelCache;
use NormCache\Cache\ModelIndexCache;
use NormCache\Cache\RelationIndexCache;
use NormCache\Cache\ResultCache;
use NormCache\Cache\VersionedPayloadStore;
use NormCache\Cache\VersionStore;
use NormCache\Payload\ModelIndexAdapter;
use NormCache\Payload\PivotIndexAdapter;
use NormCache\Payload\ResultAdapter;
use NormCache\Payload\ThroughIndexAdapter;
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
        $versions = new VersionStore($store, $keys);
        $config = new CacheConfig(
            ttl: $ttl,
            queryTtl: $queryTtl,
            cooldown: $cooldown,
            enabled: $enabled,
            fallbackEnabled: $fallback,
            dispatchEvents: $events,
            stampedeWakeTokens: $stampedeWakeTokens,
        );
        $modelCache = new ModelCache(
            $store,
            $keys,
            $versions,
            $config,
            $fireRetrieved,
            $buildingLockTtl,
            $stampedeWaitMs,
        );
        $payloads = new VersionedPayloadStore(
            $store,
            $keys,
            $versions,
            $config,
            $queryTtl,
            $buildingLockTtl,
            $stampedeWaitMs,
        );
        $invalidator = new Invalidator($store, $keys, $config, $this->spaceRegistry, $versions);
        $modelIndexes = new ModelIndexCache(
            $payloads,
            new ModelIndexAdapter,
            $modelCache,
        );
        $resultCache = new ResultCache(
            $payloads,
            new ResultAdapter($store),
            $config,
            $keys,
        );
        $relationIndexes = new RelationIndexCache(
            $payloads,
            new ThroughIndexAdapter,
            new PivotIndexAdapter($store),
            $store,
            $keys,
            $versions,
            $queryTtl,
            $buildingLockTtl,
            $stampedeWaitMs,
        );

        return new CacheManager(
            modelIndexes: $modelIndexes,
            resultCache: $resultCache,
            relationIndexes: $relationIndexes,
            modelCache: $modelCache,
            versions: $versions,
            invalidation: $invalidator,
            store: $store,
            keys: $keys,
            config: $config,
            spaceResolver: $this->spaceResolver,
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
