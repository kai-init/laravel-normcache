<?php

namespace NormCache;

use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Console\FlushCommand;
use NormCache\Debug\NormCacheCollector;
use NormCache\Debug\NormCacheDebugBarCollector;

class CacheServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/normcache.php', 'normcache');

        $this->app->singleton(CacheManager::class, fn() => new CacheManager(
            config('normcache.connection'),
            config('normcache.ttl'),
            config('normcache.query_ttl'),
            config('normcache.key_prefix'),
            config('normcache.cooldown'),
            config('normcache.cluster', false),
            config('normcache.enabled', true),
            config('normcache.events', true),
            config('normcache.fallback', false),
            config('normcache.fire_retrieved', false),
            config('normcache.building_lock_ttl', 5),
            config('normcache.stampede_wait_ms', 200),
            config('normcache.stale_ttl_depth', 3),
        ));

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
            foreach (['Laravel\Octane\Events\RequestReceived', 'Laravel\Octane\Events\TaskReceived'] as $octaneEvent) {
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
