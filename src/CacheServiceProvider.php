<?php

namespace NormCache;

use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Console\FlushCommand;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;

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

            // Reset L1 version cache and re-enable (in case fallback disabled it) between Octane requests.
            foreach (['Laravel\Octane\Events\RequestReceived', 'Laravel\Octane\Events\TaskReceived'] as $octaneEvent) {
                if (class_exists($octaneEvent)) {
                    Event::listen($octaneEvent, function () {
                        $manager = $this->app->make(CacheManager::class);
                        $manager->flushVersionLocal();
                        $manager->enable();
                    });
                }
            }

            if (config('normcache.debugbar', false) && $this->app->bound('debugbar')) {
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
        $collector = new NormCacheCollector();
        $this->app->instance('normcache.collector', $collector);
        $this->app->make('debugbar')->addCollector($collector);

        Event::listen(QueryCacheHit::class,  fn($e) => $collector->addQueryHit($e->modelClass, $e->key));
        Event::listen(QueryCacheMiss::class, fn($e) => $collector->addQueryMiss($e->modelClass, $e->key));
        Event::listen(QueryBypassed::class,  fn($e) => $collector->addBypassed($e->modelClass, $e->reasons));
        Event::listen(ModelCacheHit::class,  fn($e) => $collector->addModelHit($e->modelClass, $e->ids));
        Event::listen(ModelCacheMiss::class, fn($e) => $collector->addModelMiss($e->modelClass, $e->ids));
    }
}
