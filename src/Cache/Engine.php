<?php

namespace NormCache\Cache;

use Illuminate\Contracts\Container\Container;
use NormCache\Database\QueryBuilder;
use NormCache\Database\QueryStatement;
use NormCache\Enums\ReadOutcome;
use NormCache\Invalidator;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\QueryPlanner;
use NormCache\Support\QueryIdentity;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
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
        private Invalidator $invalidator,
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
        $connection = $query->getConnection();
        if (!$this->invalidator->reconcileOrphaned($connection)) {
            $this->observer->begin();

            return $this->bypass($query, 'transaction_active', $statement, $primaryDatabase);
        }
        if (!$this->runtime->locallyReadable()) {
            return $database();
        }

        $this->observer->begin();
        $analysis = $this->dependencies->analyze($connection, $query);
        if ($analysis->bypassReason !== null || $analysis->root === null) {
            return $this->bypass(
                $query,
                $analysis->bypassReason ?? 'unidentifiable_dependency',
                $statement,
                $database,
            );
        }

        $plan = $this->planner->plan(
            $query,
            $analysis->root,
            $analysis->tables,
            $analysis->queryScoped,
            $operation,
        );
        if ((!$plan->isDirectPrimaryKey() || $this->runtime->knownDisabled()) && !$this->runtime->readable()) {
            return $database();
        }

        $namespace = $this->identity->namespace($query->configuredTag(), $query->configuredCacheContext());
        $hash = new QueryHashResolver(fn(): string => $this->identity->hash(
            route: $plan->route,
            rootHash: $plan->root->hash,
            dependencyHashes: array_map(static fn(TableIdentity $table): string => $table->hash, $analysis->tables),
            sql: $statement->sql(),
            bindings: $statement->preparedBindings($connection),
            namespace: $namespace,
            operation: $operation,
        ));

        try {
            $cached = $this->entries->read($query, $plan, $namespace, $hash, claim: $plan->isCanonical());
            if ($cached?->served()) {
                if ($this->observer->observing()) {
                    $this->observer->read($cached->outcome, $query, $plan, $hash->value(), $statement, $cached->reason);
                }

                return $cached->rows;
            }
        } catch (\InvalidArgumentException) {
            return $this->bypass($query, 'unsupported_query_shape', $statement, $database, $plan);
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return $database();
        }

        if ($cached === null) {
            return $database();
        }

        try {
            $queryHash = $hash->value();
            $leases = $this->container->make(BuildLeaseCoordinator::class);
            $lease = $cached->lease ?? $leases->claim($plan, $cached->state, $namespace, $queryHash);
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return $database();
        }

        $this->observer->read(ReadOutcome::MISS, $query, $plan, $queryHash, $statement, $cached->reason);
        if (!$lease->owner) {
            try {
                if ($lease->wakeKey !== null) {
                    $this->store->brpop($lease->wakeKey, $this->config->stampedeWaitMs / 1000);
                    $retry = $this->entries->read($query, $plan, $namespace, $hash);
                    if ($retry?->served()) {
                        $this->observer->read($retry->outcome, $query, $plan, $queryHash, $statement, $retry->reason);

                        return $retry->rows;
                    }
                }
            } catch (\Throwable $exception) {
                $this->runtime->fail($exception);

                return $database();
            }

            return $primaryDatabase();
        }

        $rows = null;
        $reused = [];
        $leaseSettled = false;
        $twoPhase = $plan->isCanonical() && $cached->reason === null && $query->limit !== 1;
        try {
            if ($twoPhase) {
                $loaded = $this->container->make(RowRepairer::class)->loadCanonical($query, $plan, $cached->state);
                $rows = $loaded?->rows;
                $reused = $loaded->reusedTokens ?? [];
            } else {
                $rows = $primaryDatabase();
            }

            if ($rows !== null) {
                try {
                    if (
                        ($cached->state->versions === [] && $cached->state->tagKey === null)
                        || $this->states->resolve($plan, $namespace, $queryHash)->equals($cached->state)
                    ) {
                        // A returned publish settles the lease (locally or atomically in Lua).
                        $this->entries->publish($query, $plan, $cached->state, $rows, $lease, $reused);
                        $leaseSettled = true;
                    } elseif ($twoPhase) {
                        $rows = null;
                    }
                } catch (\Throwable $exception) {
                    $this->runtime->fail($exception);
                }
            }
        } finally {
            if (!$leaseSettled) {
                $leases->release($lease);
            }
        }

        return $rows ?? $primaryDatabase();
    }

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
}
