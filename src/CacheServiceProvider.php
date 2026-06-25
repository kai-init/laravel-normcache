<?php

namespace NormCache;

use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Cache\ExecutionEngine;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\NormalizedCacheReader;
use NormCache\Cache\NormalizedThroughReader;
use NormCache\Cache\ResultCacheReader;
use NormCache\Cache\ResultExecutor;
use NormCache\Cache\VersionTracker;
use NormCache\Console\FlushCommand;
use NormCache\Debug\NormCacheCollector;
use NormCache\Debug\NormCacheDebugBarCollector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;

class CacheServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/normcache.php', 'normcache');

        $this->app->singleton(CacheManager::class, function () {
            $connection = config('normcache.connection');
            $ttl = (int) config('normcache.ttl');
            $queryTtl = (int) config('normcache.query_ttl');
            $keyPrefix = config('normcache.key_prefix');
            $cooldown = (int) config('normcache.cooldown');
            $cluster = (bool) config('normcache.cluster', false);
            $enabled = (bool) config('normcache.enabled', true);
            $events = (bool) config('normcache.events', false);
            $fallback = (bool) config('normcache.fallback', true);
            $fireRetrieved = (bool) config('normcache.fire_retrieved', false);
            $buildingLockTtl = (int) config('normcache.building_lock_ttl', 5);
            $stampedeWaitMs = (int) config('normcache.stampede_wait_ms', 200);
            $stampedeWakeTokens = (int) config('normcache.stampede_wake_tokens', 64);
            $slotting = (bool) config('normcache.slotting', false);

            $slottingActive = $cluster && $slotting;
            $store = new RedisStore($connection, $keyPrefix, $slottingActive, $slotting ? '' : '{nc}:', $stampedeWakeTokens);
            $keys = new CacheKeyBuilder;
            $versions = new VersionTracker($store, $keys);
            $resultReader = new ResultCacheReader($store, $keys, $versions, $queryTtl, $buildingLockTtl, $stampedeWaitMs, $slottingActive, $stampedeWakeTokens);
            $engine = new ExecutionEngine;
            $config = new CacheConfig(
                ttl: $ttl,
                queryTtl: $queryTtl,
                cooldown: $cooldown,
                enabled: $enabled,
                fallbackEnabled: $fallback,
                dispatchEvents: $events,
                cluster: $cluster,
                slotting: $slottingActive,
                stampedeWakeTokens: $stampedeWakeTokens,
            );

            return new CacheManager(
                queryReader: new NormalizedCacheReader($store, $keys, $versions, $queryTtl, $buildingLockTtl, $stampedeWaitMs, $slottingActive, $stampedeWakeTokens),
                resultReader: $resultReader,
                throughReader: new NormalizedThroughReader($store, $keys, $versions, $queryTtl, $buildingLockTtl, $stampedeWaitMs, $slottingActive, $stampedeWakeTokens),
                result: new ResultExecutor($engine, $resultReader, $config),
                hydrator: new ModelHydrator($store, $keys, $versions, $ttl, $fireRetrieved, $buildingLockTtl, $stampedeWaitMs),
                versions: $versions,
                engine: $engine,
                store: $store,
                keys: $keys,
                config: $config,
            );
        });

        $this->app->alias(CacheManager::class, 'normcache');
    }

    public function boot(): void
    {
        if (config('normcache.enabled', true)) {
            Event::listen(TransactionCommitted::class, function (TransactionCommitted $event) {
                if ($event->connection->transactionLevel() === 0) {
                    $this->app->make(CacheManager::class)->commitPending($event->connection->getName());
                }
            });

            Event::listen(TransactionRolledBack::class, function (TransactionRolledBack $event) {
                if ($event->connection->transactionLevel() === 0) {
                    $this->app->make(CacheManager::class)->discardPending($event->connection->getName());
                }
            });

            // Re-enable optimistically between queue jobs. If Redis is still down, fallback() will
            // disable again on the first failed call — worst case is one extra Redis attempt per job.
            $resetManager = function () {
                $manager = $this->app->make(CacheManager::class);
                $manager->discardAllPending();
                $manager->enable();
            };

            Event::listen(JobProcessed::class, $resetManager);
            Event::listen(Looping::class, $resetManager);

            // Re-enable (in case fallback disabled it) between Octane requests.
            foreach (['RequestReceived', 'TaskReceived'] as $event) {
                $octaneEvent = "Laravel\\Octane\\Events\\$event";
                if (class_exists($octaneEvent)) {
                    Event::listen($octaneEvent, $resetManager);
                }
            }

            if (config('normcache.debugbar', false) && $this->debugbarIsEnabled()) {
                $this->registerDebugbarCollector();
            }
        }

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/normcache.php' => config_path('normcache.php'),
            ], 'normcache-config');

            $this->commands([FlushCommand::class]);
        }
    }

    private function registerDebugbarCollector(): void
    {
        $collector = new NormCacheDebugBarCollector;
        NormCacheCollector::register($collector);
        $this->app->make('debugbar')->addCollector($collector);
    }

    private function debugbarIsEnabled(): bool
    {
        if (!$this->app->bound('debugbar')) {
            return false;
        }

        $debugbar = $this->app->make('debugbar');

        return !method_exists($debugbar, 'isEnabled') || $debugbar->isEnabled();
    }
}
