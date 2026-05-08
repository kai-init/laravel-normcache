<?php

namespace NormCache;

use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Console\FlushCommand;

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

            // Reset L1 version cache between requests when running under Octane.
            foreach (['Laravel\Octane\Events\RequestReceived', 'Laravel\Octane\Events\TaskReceived'] as $octaneEvent) {
                if (class_exists($octaneEvent)) {
                    Event::listen($octaneEvent, fn() => $this->app->make(CacheManager::class)->flushVersionLocal());
                }
            }
        }

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/normcache.php' => config_path('normcache.php'),
            ], 'normcache-config');

            $this->commands([FlushCommand::class]);
        }
    }
}
