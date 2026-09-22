<?php

namespace NormCache\Cache;

use Illuminate\Database\Connection;
use NormCache\Database\QueryBuilder;
use NormCache\Database\QueryStatement;
use NormCache\Enums\ReadOutcome;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\QueryPlanner;
use NormCache\Support\QueryIdentity;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final readonly class Engine
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private QueryPlanner $planner,
        private QueryIdentity $identity,
        private DependencyAnalyzer $dependencies,
        private QueryObserver $observer,
        private CacheStateResolver $states,
        private QueryEntryRepository $entries,
        private BuildLeaseCoordinator $leases,
        private CacheReader $reader,
        private CanonicalRowRepository $rows,
    ) {}

    /**
     * @param  callable(): array  $database
     * @param  callable(): array  $primaryDatabase
     */
    public function select(
        QueryBuilder $query,
        QueryStatement $statement,
        string $operation,
        callable $database,
        callable $primaryDatabase,
    ): array {
        if (!$this->runtime->readable()) {
            return $database();
        }

        $this->observer->begin();

        $connection = $query->getConnection();
        $analysis = $this->dependencies->analyze($connection, $query);
        $table = $analysis->root;

        if ($analysis->bypassReason !== null || $table === null) {
            return $this->bypass(
                $query,
                $analysis->bypassReason ?? 'unidentifiable_dependency',
                $statement,
                $database,
            );
        }

        $dependencies = $analysis->tables;

        $plan = $this->planner->plan(
            $query,
            $table,
            $dependencies,
            $analysis->queryScoped,
            $operation,
        );

        $namespace = $this->identity->namespace(
            $query->configuredTag(),
            $query->configuredCacheContext(),
        );
        $context = new ReadContext($query, $plan, $namespace);
        $dependencyHashes = array_map(
            static fn(TableIdentity $dependency): string => $dependency->hash,
            $dependencies,
        );
        $hash = $this->queryHashResolver(
            $context,
            $connection,
            $dependencyHashes,
            $operation,
            $statement,
        );

        try {
            $cached = $this->reader->read($context, $hash);

            if ($cached->served()) {
                if ($this->observer->observing()) {
                    $this->reportRead(
                        $context,
                        $hash->value(),
                        $statement,
                        $cached,
                    );
                }

                return $cached->rows;
            }
        } catch (\InvalidArgumentException) {
            return $this->bypass(
                $query,
                'unsupported_query_shape',
                $statement,
                $database,
                $plan,
            );
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return $database();
        }

        try {
            $queryHash = $hash->value();
            $lease = $this->leases->claim($context->plan, $cached->state, $context->namespace, $queryHash);
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return $database();
        }

        $this->observer->miss(
            $context->query,
            $context->plan,
            $queryHash,
            $statement,
            $cached->reason,
        );

        if (!$lease->owner) {
            if ($lease->wakeKey !== null) {
                try {
                    $this->store->brpop(
                        $lease->wakeKey,
                        $this->config->stampedeWaitMs / 1000,
                    );
                    $retry = $this->reader->read($context, $hash);

                    if ($retry->served()) {
                        $this->reportRead(
                            $context,
                            $queryHash,
                            $statement,
                            $retry,
                        );

                        return $retry->rows;
                    }
                } catch (\Throwable $exception) {
                    $this->runtime->fail($exception);

                    return $database();
                }
            }

            return $primaryDatabase();
        }

        if ($plan->isCanonical() && $cached->reason === null) {
            try {
                $rows = $this->entries->loadCanonical($query, $plan, $cached->state);
            } catch (\Throwable $exception) {
                $this->leases->release($lease);
                $this->runtime->fail($exception);

                return $primaryDatabase();
            }

            if ($rows === null) {
                $this->leases->release($lease);

                return $primaryDatabase();
            }
        } else {
            try {
                $rows = $primaryDatabase();
            } catch (\Throwable $exception) {
                $this->leases->release($lease);

                throw $exception;
            }
        }

        try {
            if (
                ($cached->state->versions === [] && $cached->state->tagKey === null)
                || $this->states->resolve($context->plan, $context->namespace, $queryHash)->equals($cached->state)
            ) {
                $this->publish($context, $cached->state, $rows, $lease);
            } else {
                $this->leases->release($lease);
            }
        } catch (\Throwable $exception) {
            $this->leases->release($lease);
            $this->runtime->fail($exception);
        }

        return $rows;
    }

    /**
     * @param  callable(): array  $database
     */
    private function bypass(
        QueryBuilder $query,
        string $reason,
        QueryStatement $statement,
        callable $database,
        ?QueryPlan $plan = null,
    ): array {
        $this->observer->bypass($query, $reason, $statement, $plan);

        return $database();
    }

    /** @param list<string> $dependencyHashes */
    private function queryHashResolver(
        ReadContext $context,
        Connection $connection,
        array $dependencyHashes,
        string $operation,
        QueryStatement $statement,
    ): QueryHashResolver {
        return new QueryHashResolver(fn(): string => $this->identity->hash(
            route: $context->plan->route,
            rootHash: $context->plan->root->hash,
            dependencyHashes: $dependencyHashes,
            sql: $statement->sql(),
            bindings: $statement->preparedBindings($connection),
            namespace: $context->namespace,
            operation: $operation,
        ));
    }

    private function reportRead(
        ReadContext $context,
        string $queryHash,
        QueryStatement $statement,
        CacheRead $read,
    ): void {
        if ($read->outcome === ReadOutcome::REPAIRED) {
            $this->observer->repaired(
                $context->query,
                $context->plan,
                $queryHash,
                $statement,
                $read->reason,
            );

            return;
        }

        $this->observer->hit(
            $context->query,
            $context->plan,
            $queryHash,
            $statement,
            $read->reason,
        );
    }

    /** @param array<int, mixed> $rows */
    private function publish(
        ReadContext $context,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        if ($context->plan->isCanonical()) {
            $this->publishCanonical($context, $state, $rows, $lease);

            return;
        }

        if ($context->plan->isDirectPrimaryKey()) {
            $this->rows->publish($context->plan, $state, $rows, $lease);

            return;
        }

        if (!$this->entries->publishResult(
            $context->query,
            $context->plan,
            $state,
            $rows,
            $lease,
            $this->config->wakeTtl(),
        )) {
            $this->leases->release($lease);
        }
    }

    private function publishCanonical(
        ReadContext $context,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        $overlay = $this->entries->inlineResult($state, $rows);

        if (!$this->entries->publishCanonical(
            $context->query,
            $context->plan,
            $state,
            $rows,
            $lease,
            $this->config->wakeTtl(),
            $overlay,
        )) {
            $this->leases->release($lease);
        }
    }
}
