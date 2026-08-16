<?php

namespace NormCache;

use DebugBar\DataCollector\TimeDataCollector;
use Illuminate\Database\Events\MigrationsEnded;
use Illuminate\Database\Events\TransactionBeginning;
use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\CacheStateResolver;
use NormCache\Cache\CanonicalRowRepository;
use NormCache\Cache\Engine;
use NormCache\Cache\MembershipRevalidator;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Cache\RowRepairer;
use NormCache\Console\DisableCommand;
use NormCache\Console\EnableCommand;
use NormCache\Console\FlushCommand;
use NormCache\Debug\DebugBarCollector;
use NormCache\Payload\ChangeRecordCodec;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\DeleteDependencyResolver;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheSerializer;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryIdentity;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;

final class CacheServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/normcache.php', 'normcache');

        $this->app->singleton(CacheConfig::class, fn() => CacheConfig::fromArray(
            (array) config('normcache', []),
        ));
        $this->app->singleton(CacheKeyBuilder::class, fn($app) => new CacheKeyBuilder(
            $app->make(CacheConfig::class)->keyPrefix,
        ));
        $this->app->singleton(RedisStore::class, fn($app) => new RedisStore(
            $app->make(CacheConfig::class)->connection,
        ));
        $this->app->singleton(CacheSerializer::class, fn($app) => new CacheSerializer(
            $app->make(CacheConfig::class)->serializer,
        ));
        $this->app->singleton(RawResultCodec::class);
        $this->app->singleton(ChangeRecordCodec::class);
        $this->app->singleton(MembershipCodec::class);
        $this->app->singleton(QueryIdentity::class);
        $this->app->singleton(QueryPlanner::class);
        $this->app->singleton(MutationKeyExtractor::class);
        $this->app->scoped(QueryObserver::class, function ($app): QueryObserver {
            $config = $app->make(CacheConfig::class);
            $collector = null;

            if ($config->debugbar && $app->bound('debugbar') && class_exists(TimeDataCollector::class)) {
                $debugbar = $app->make('debugbar');

                if (!method_exists($debugbar, 'isEnabled') || $debugbar->isEnabled()) {
                    $collector = new DebugBarCollector;
                    $debugbar->addCollector($collector);
                }
            }

            return new QueryObserver($config, $collector, $app->make(FailureReporter::class));
        });

        $this->app->singleton(TableIdentityResolver::class);
        $this->app->singleton(DeleteDependencyResolver::class);
        $this->app->singleton(DependencyAnalyzer::class);

        $this->app->scoped(FailureReporter::class);
        $this->app->scoped(CacheRuntime::class);
        $this->app->scoped(CacheStateResolver::class);
        $this->app->scoped(BuildLeaseCoordinator::class);
        $this->app->scoped(RowRepairer::class);
        $this->app->scoped(CanonicalRowRepository::class);
        $this->app->scoped(QueryEntryRepository::class);
        $this->app->singleton(MembershipRevalidator::class);
        $this->app->scoped(Invalidator::class);
        $this->app->scoped(Engine::class);
        $this->app->scoped(CacheManager::class);
        $this->app->alias(CacheManager::class, 'normcache');
    }

    public function boot(): void
    {
        Event::listen(TransactionBeginning::class, function (TransactionBeginning $event): void {
            $name = (string) $event->connection->getName();

            // A thrown commit emits no completion event and may have reached the database.
            if ($event->connection->transactionLevel() === 1) {
                $this->app->make(Invalidator::class)->commit($name);
            }

            // Registering per level keeps invalidation ahead of any afterCommit
            // callback the application adds at that same nesting level.
            try {
                $event->connection->afterCommit(function () use ($name): void {
                    $this->app->make(Invalidator::class)->commit($name);
                });
            } catch (\Throwable $exception) {
                $this->app->make(FailureReporter::class)->cacheUnavailable($exception);
            }
        });
        Event::listen(TransactionCommitted::class, function (TransactionCommitted $event): void {
            if ($event->connection->transactionLevel() === 0) {
                $this->app->make(Invalidator::class)->commit((string) $event->connection->getName());
            }
        });
        Event::listen(TransactionRolledBack::class, function (TransactionRolledBack $event): void {
            if ($event->connection->transactionLevel() === 0) {
                $this->app->make(Invalidator::class)->rollback((string) $event->connection->getName());
            }
        });
        Event::listen(MigrationsEnded::class, function (): void {
            $this->app->make(CacheManager::class)->flushAll();
        });

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/normcache.php' => config_path('normcache.php'),
            ], 'normcache-config');

            $this->commands([FlushCommand::class, DisableCommand::class, EnableCommand::class]);
        }
    }
}
