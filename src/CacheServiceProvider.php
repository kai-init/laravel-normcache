<?php

namespace NormCache;

use DebugBar\DataCollector\TimeDataCollector;
use Illuminate\Database\Connection;
use Illuminate\Database\Events\MigrationsEnded;
use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use NormCache\Cache\CacheStateResolver;
use NormCache\Cache\CacheSwitch;
use NormCache\Cache\CanonicalRepository;
use NormCache\Cache\Engine;
use NormCache\Cache\ResultRepository;
use NormCache\Console\DisableCommand;
use NormCache\Console\EnableCommand;
use NormCache\Console\FlushCommand;
use NormCache\Database\Connections\MariaDbConnection;
use NormCache\Database\Connections\MySqlConnection;
use NormCache\Database\Connections\PostgresConnection;
use NormCache\Database\Connections\SQLiteConnection;
use NormCache\Database\Connections\SqlServerConnection;
use NormCache\Debug\DebugBarCollector;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheSerializer;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Support\Reporter;
use NormCache\Values\CacheConfig;
use NormCache\Values\RuntimeState;
use Psr\Log\LoggerInterface;

final class CacheServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/normcache.php', 'normcache');
        $this->registerConnectionResolvers();

        $this->app->singleton(CacheConfig::class, fn() => CacheConfig::fromArray(
            (array) config('normcache', []),
        ));
        $this->app->singleton(CacheKeyBuilder::class, fn($app) => new CacheKeyBuilder(
            $app->make(CacheConfig::class)->keyPrefix,
        ));
        $this->app->singleton(RedisStore::class, fn($app) => new RedisStore(
            $app->make(CacheConfig::class)->connection,
            $app->make(CacheConfig::class)->stampedeWakeTokens,
        ));
        $this->app->singleton(CacheSerializer::class, fn() => CacheSerializer::native());
        $this->app->singleton(RawResultCodec::class);
        $this->app->singleton(MembershipCodec::class);
        $this->app->singleton(QueryIdentity::class);
        $this->app->singleton(QueryPlanner::class);
        $this->app->singleton(TableIdentityResolver::class);
        $this->app->singleton(DependencyAnalyzer::class);
        $this->app->singleton(PrimaryKeyResolver::class);
        $this->app->singleton(MutationKeyExtractor::class);
        $this->app->scoped(Reporter::class, function ($app): Reporter {
            $config = $app->make(CacheConfig::class);
            $collector = null;

            if ($config->debugbar && $app->bound('debugbar') && class_exists(TimeDataCollector::class)) {
                $debugbar = $app->make('debugbar');

                if (!method_exists($debugbar, 'isEnabled') || $debugbar->isEnabled()) {
                    $collector = new DebugBarCollector;
                    $debugbar->addCollector($collector);
                }
            }

            return new Reporter($config, $collector, $app->make(RuntimeState::class));
        });

        $this->app->scoped(RuntimeState::class);
        $this->app->scoped(CacheSwitch::class);
        $this->app->scoped(CacheStateResolver::class);
        $this->app->scoped(CanonicalRepository::class);
        $this->app->scoped(ResultRepository::class);
        $this->app->scoped(Invalidator::class);
        $this->app->scoped(Engine::class);
        $this->app->scoped(CacheManager::class);
        $this->app->alias(CacheManager::class, 'normcache');
    }

    public function boot(): void
    {
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
            $cache = $this->app->make(CacheManager::class);
            $cache->clearSchemaMetadata();

            if (!$cache->flushAll()) {
                $this->app->make(LoggerInterface::class)->warning(
                    'NormCache could not advance its epoch after migrations completed. Run normcache:flush before enabling cache traffic.',
                );
            }
        });

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/normcache.php' => config_path('normcache.php'),
            ], 'normcache-config');

            $this->commands([FlushCommand::class, DisableCommand::class, EnableCommand::class]);
        }
    }

    private function registerConnectionResolvers(): void
    {
        $resolvers = [
            'mysql' => MySqlConnection::class,
            'mariadb' => MariaDbConnection::class,
            'pgsql' => PostgresConnection::class,
            'sqlite' => SQLiteConnection::class,
            'sqlsrv' => SqlServerConnection::class,
        ];

        foreach ($resolvers as $driver => $class) {
            $existing = Connection::getResolver($driver);

            if ($existing !== null) {
                $reflection = new \ReflectionFunction($existing);

                if ($reflection->getFileName() !== __FILE__) {
                    $this->app->make(LoggerInterface::class)->warning(
                        'NormCache did not replace an existing database connection resolver.',
                        ['driver' => $driver],
                    );
                }

                continue;
            }

            Connection::resolverFor(
                $driver,
                static fn($connection, $database, $prefix, $config) => new $class(
                    $connection,
                    $database,
                    $prefix,
                    $config,
                ),
            );
        }
    }
}
