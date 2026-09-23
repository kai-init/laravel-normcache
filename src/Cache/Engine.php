<?php

namespace NormCache\Cache;

use Illuminate\Contracts\Container\Container;
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
        private Container $container,
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
        if (!$this->runtime->locallyReadable()) {
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

        if (!$plan->isDirectPrimaryKey() && !$this->runtime->readable()) {
            return $database();
        }

        $dependencyHashes = array_map(
            static fn(TableIdentity $dependency): string => $dependency->hash,
            $dependencies,
        );
        $queryHash = null;
        $hash = function () use (
            &$queryHash,
            $plan,
            $connection,
            $dependencyHashes,
            $namespace,
            $operation,
            $statement,
        ): string {
            return $queryHash ??= $this->identity->hash(
                route: $plan->route,
                rootHash: $plan->root->hash,
                dependencyHashes: $dependencyHashes,
                sql: $statement->sql(),
                bindings: $statement->preparedBindings($connection),
                namespace: $namespace,
                operation: $operation,
            );
        };

        try {
            $cached = $this->read($query, $plan, $namespace, $hash);

            if ($plan->isDirectPrimaryKey() && !$this->runtime->readable()) {
                return $database();
            }

            if ($cached->served()) {
                if ($this->observer->observing()) {
                    $this->observer->read(
                        $cached->outcome,
                        $query,
                        $plan,
                        $hash(),
                        $statement,
                        $cached->reason,
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
            $queryHash = $hash();
            $lease = $this->leases()->claim($plan, $cached->state, $namespace, $queryHash);
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return $database();
        }

        $this->observer->read(
            ReadOutcome::MISS,
            $query,
            $plan,
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
                    $retry = $this->read($query, $plan, $namespace, $hash);

                    if ($retry->served()) {
                        $this->observer->read(
                            $retry->outcome,
                            $query,
                            $plan,
                            $queryHash,
                            $statement,
                            $retry->reason,
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
                $this->leases()->release($lease);
                $this->runtime->fail($exception);

                return $primaryDatabase();
            }

            if ($rows === null) {
                $this->leases()->release($lease);

                return $primaryDatabase();
            }
        } else {
            try {
                $rows = $primaryDatabase();
            } catch (\Throwable $exception) {
                $this->leases()->release($lease);

                throw $exception;
            }
        }

        try {
            if (
                ($cached->state->versions === [] && $cached->state->tagKey === null)
                || $this->states->resolve($plan, $namespace, $queryHash)->equals($cached->state)
            ) {
                $this->publish($query, $plan, $cached->state, $rows, $lease);
            } else {
                $this->leases()->release($lease);
            }
        } catch (\Throwable $exception) {
            $this->leases()->release($lease);
            $this->runtime->fail($exception);
        }

        return $rows;
    }

    /** @param \Closure(): string $hash */
    private function read(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        \Closure $hash,
    ): CacheRead {
        return $plan->isDirectPrimaryKey()
            ? $this->readDirect($plan, $namespace, $hash)
            : $this->entries->read($query, $plan, $namespace, $hash());
    }

    /** @param \Closure(): string $hash */
    private function readDirect(
        QueryPlan $plan,
        string $namespace,
        \Closure $hash,
    ): CacheRead {
        $cached = $this->rows->read($plan);
        $resolve = fn(): CacheState => $this->states->resolve(
            $plan,
            $namespace,
            $hash(),
            knownGeneration: $cached->generation,
        );

        if ($cached->row === null) {
            return new CacheRead($resolve(), ReadOutcome::MISS, [], $cached->reason);
        }

        $rows = $this->rows->visibleRows($plan, $cached->row);

        return $rows === null
            ? new CacheRead($resolve(), ReadOutcome::MISS, [], 'corrupt_payload')
            : new CacheRead(
                $this->rows->state($plan, $cached->generation, (string) $cached->epoch),
                ReadOutcome::HIT,
                $rows,
            );
    }

    private function leases(): BuildLeaseCoordinator
    {
        return $this->container->make(BuildLeaseCoordinator::class);
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

    /** @param array<int, mixed> $rows */
    private function publish(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        if ($plan->isCanonical()) {
            $this->publishCanonical($query, $plan, $state, $rows, $lease);

            return;
        }

        if ($plan->isDirectPrimaryKey()) {
            $this->rows->publish($plan, $state, $rows, $lease);

            return;
        }

        if (!$this->entries->publishResult(
            $query,
            $plan,
            $state,
            $rows,
            $lease,
            $this->config->wakeTtl(),
        )) {
            $this->leases()->release($lease);
        }
    }

    /** @param array<int, mixed> $rows */
    private function publishCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        $overlay = $this->entries->inlineResult($state, $rows);

        if (!$this->entries->publishCanonical(
            $query,
            $plan,
            $state,
            $rows,
            $lease,
            $this->config->wakeTtl(),
            $overlay,
        )) {
            $this->leases()->release($lease);
        }
    }
}
