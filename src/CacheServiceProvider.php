<?php

namespace NormCache;

use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\Looping;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Cache\ModelsExecutor;
use NormCache\Console\FlushCommand;
use NormCache\Debug\NormCacheCollector;
use NormCache\Debug\NormCacheDebugBarCollector;
use NormCache\Planning\CachePlanner;
use NormCache\Planning\CachePlanSpaceValidator;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;

class CacheServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/normcache.php', 'normcache');

        $this->app->singleton(CacheSpaceRegistry::class, function () {
            $metadataStore = new RedisStore((string) config('normcache.connection'), (int) config('normcache.stampede_wake_tokens', 64));

            return new CacheSpaceRegistry(
                maxPerModel: (int) config('normcache.spaces.max_per_model', 16),
                placement: (array) config('normcache.spaces.placement', []),
                metadataStore: $metadataStore,
                metadataKeyPrefix: (string) config('normcache.key_prefix', ''),
            );
        });

        $this->app->singleton(CacheSpaceResolver::class, function ($app) {
            return new CacheSpaceResolver($app->make(CacheSpaceRegistry::class));
        });

        $this->app->singleton(CachePlanSpaceValidator::class, function ($app) {
            return new CachePlanSpaceValidator(
                registry: $app->make(CacheSpaceRegistry::class),
                resolver: $app->make(CacheSpaceResolver::class),
                crossSpaceBehavior: (string) config('normcache.spaces.cross_space_behavior', 'bypass'),
                debug: (bool) config('app.debug', false),
                logger: $app->make('log'),
            );
        });

        $this->app->singleton(CachePlanner::class, function ($app) {
            return new CachePlanner(spaceValidator: $app->make(CachePlanSpaceValidator::class));
        });

        $this->app->singleton(ModelsExecutor::class);

        $this->app->singleton(CacheManagerFactory::class, function ($app) {
            return new CacheManagerFactory(
                $app->make(CacheSpaceRegistry::class),
                $app->make(CacheSpaceResolver::class),
            );
        });

        $this->app->scoped(CacheManager::class, fn($app) => $app->make(CacheManagerFactory::class)->make());

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
            Event::listen(JobProcessed::class, CacheKeyBuilder::reset(...));
            Event::listen(Looping::class, CacheKeyBuilder::reset(...));

            // Reset static metadata between Octane requests and tasks.
            foreach (['RequestReceived', 'TaskReceived'] as $event) {
                $octaneEvent = "Laravel\\Octane\\Events\\$event";
                if (class_exists($octaneEvent)) {
                    Event::listen($octaneEvent, CacheKeyBuilder::reset(...));
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
