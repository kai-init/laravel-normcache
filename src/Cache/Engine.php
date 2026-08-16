<?php

namespace NormCache\Cache;

use Illuminate\Database\Connection;
use NormCache\Database\QueryBuilder;
use NormCache\Database\QueryStatement;
use NormCache\Enums\ReadOutcome;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\QueryPlanner;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\QueryIdentity;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisProtocol;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\RowRepair;
use NormCache\Values\TableIdentity;

final readonly class Engine
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private QueryPlanner $planner,
        private QueryIdentity $identity,
        private DependencyAnalyzer $dependencies,
        private QueryObserver $observer,
        private CacheStateResolver $states,
        private QueryEntryRepository $entries,
        private BuildLeaseCoordinator $leases,
        private RowRepairer $repairer,
        private CanonicalRowRepository $rows,
        private MembershipRevalidator $revalidator,
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
        if ($query->isInternal()) {
            return $database();
        }

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

        if ($analysis->volatile) {
            return $this->bypass(
                $query,
                'volatile_expression',
                $statement,
                $database,
                $plan,
            );
        }

        $namespace = $this->identity->namespace(
            $query->configuredTag(),
            $query->configuredCacheContext(),
        );
        $context = new ReadContext($query, $plan, $namespace);
        $canonicalQueryHash = null;
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
            $canonicalQueryHash = $this->resultOverlayCanonicalHash(
                $context,
                $connection,
                $dependencyHashes,
                $statement,
            );
            $cached = $this->readCache(
                $context,
                $hash,
                $canonicalQueryHash,
            );

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
            $revalidated = $this->revalidate($context, $cached, $queryHash, $statement);

            if ($revalidated !== null) {
                return $revalidated;
            }

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
                    // Direct-PK waiters retry through the row-key branch.
                    $retry = $this->readCache(
                        $context,
                        $hash,
                        $canonicalQueryHash,
                    );

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

        try {
            $rows = $primaryDatabase();
        } catch (\Throwable $exception) {
            $this->leases->release($lease);

            throw $exception;
        }

        try {
            // Publish scripts validate state while holding the lease.
            $this->publish($context, $cached->state, $rows, $lease);
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
        if ($context->plan->isCanonical()) {
            return new QueryHashResolver(fn(): string => $this->canonicalQueryHash(
                $context,
                $connection,
                $dependencyHashes,
                $statement,
            ));
        }

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

    /** @param list<string> $dependencyHashes */
    private function resultOverlayCanonicalHash(
        ReadContext $context,
        Connection $connection,
        array $dependencyHashes,
        QueryStatement $statement,
    ): ?string {
        if (!$context->plan->supportsCanonicalProjectionFallback()) {
            return null;
        }

        return $this->canonicalQueryHash($context, $connection, $dependencyHashes, $statement);
    }

    private function readCache(
        ReadContext $context,
        QueryHashResolver $hash,
        ?string $canonicalQueryHash,
    ): CacheRead {
        if ($context->plan->isDirectPrimaryKey()) {
            return $this->readDirect($context, $hash);
        }

        return $this->read(
            $context,
            $hash->value(),
            $canonicalQueryHash,
        );
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

    private function read(
        ReadContext $context,
        string $queryHash,
        ?string $canonicalQueryHash = null,
    ): CacheRead {
        if ($context->plan->isCanonical()) {
            return $this->config->maxAutoOverlayRows > 0
                ? $this->readCanonicalWithResultOverlay(
                    $context,
                    $queryHash,
                )
                : $this->readCanonical($context, $queryHash);
        }

        if ($context->plan->isQueryGroup()) {
            $entryKey = $this->keys->queryGroupEntry($queryHash, $context->namespace);
            [$raw, $values] = $this->store->readHashFieldWithValues(
                $entryKey,
                'r',
                $this->states->pendingStateKeys($context->plan, $context->namespace),
            );
            $state = $this->states->resolve(
                $context->plan,
                $context->namespace,
                $queryHash,
                prefetched: $values,
            );

            return $this->entries->readResult($state, $raw);
        }

        if ($canonicalQueryHash !== null && $context->plan->supportsCanonicalProjectionFallback()) {
            return $this->readResultOrCanonicalProjection(
                $context,
                $queryHash,
                $canonicalQueryHash,
            );
        }

        $entry = $this->store->fetchResult(
            $this->keys->version($context->plan->root),
            $this->keys->tablePrefix($context->plan->root),
            $context->namespace,
            $queryHash,
        );
        $version = RedisProtocol::version($entry, 0);
        $raw = RedisProtocol::value($entry, 1);

        if (!is_string($raw) && $context->plan->supportsRowFallback()) {
            $fallback = $this->readResultRowFallback($context->plan);

            if ($fallback !== null) {
                return $fallback;
            }
        }

        $state = $this->states->resolve($context->plan, $context->namespace, $queryHash, $version);

        return $this->entries->readResult($state, $raw);
    }

    private function readCanonicalWithResultOverlay(
        ReadContext $context,
        string $queryHash,
    ): CacheRead {
        $head = $this->store->fetchResultOrCanonical(
            versionKey: $this->keys->version($context->plan->root),
            generationKey: $this->keys->generation($context->plan->root),
            tablePrefix: $this->keys->tablePrefix($context->plan->root),
            namespace: $context->namespace,
            resultQueryHash: $queryHash,
            canonicalQueryHash: $queryHash,
        );
        $status = RedisProtocol::status($head);
        $version = RedisProtocol::version($head);

        if ($status === RedisProtocol::RESULT) {
            $state = $this->states->resolve(
                $context->plan,
                $context->namespace,
                $queryHash,
                $version,
                usesGeneration: false,
            );
            $result = $this->entries->readResult($state, RedisProtocol::resultPayload($head));

            if ($result->served()) {
                return $result->withReason('result_overlay');
            }

            $overlayReason = $result->reason;
            $canonicalResult = $this->readCanonicalHead(
                $context,
                $queryHash,
                $this->canonicalHeadFrom($head, $version),
                true,
            );

            if ($canonicalResult->promotable()) {
                $promoted = $this->entries->promoteResult(
                    $context->query,
                    $context->plan,
                    $canonicalResult->state,
                    $context->namespace,
                    $queryHash,
                    $canonicalResult->rows,
                );

                if ($overlayReason === 'corrupt_payload') {
                    $canonicalResult = $this->entries->rebuiltResultOutcome(
                        $canonicalResult,
                        $promoted,
                    );
                }
            }

            return $canonicalResult;
        }

        $generation = RedisProtocol::version($head, 2);
        $canonicalHead = $status === RedisProtocol::MEMBERSHIP
            ? [
                RedisProtocol::HIT,
                $version,
                $generation,
                RedisProtocol::canonicalPayload($head),
            ]
            : [$status, $version, $generation];
        $result = $this->readCanonicalHead(
            $context,
            $queryHash,
            $canonicalHead,
            true,
        );

        if ($result->promotable()) {
            $this->entries->promoteResult(
                $context->query,
                $context->plan,
                $result->state,
                $context->namespace,
                $queryHash,
                $result->rows,
            );
        }

        return $result;
    }

    private function readResultOrCanonicalProjection(
        ReadContext $context,
        string $queryHash,
        string $canonicalQueryHash,
    ): CacheRead {
        $head = $this->store->fetchResultOrCanonical(
            versionKey: $this->keys->version($context->plan->root),
            generationKey: $this->keys->generation($context->plan->root),
            tablePrefix: $this->keys->tablePrefix($context->plan->root),
            namespace: $context->namespace,
            resultQueryHash: $queryHash,
            canonicalQueryHash: $canonicalQueryHash,
        );
        $status = RedisProtocol::status($head);
        $version = RedisProtocol::version($head);

        if ($status !== RedisProtocol::RESULT) {
            return $this->readCanonicalProjectionFallback(
                $context,
                $queryHash,
                $canonicalQueryHash,
                $head,
                null,
            );
        }

        $state = $this->states->resolve($context->plan, $context->namespace, $queryHash, $version);
        $result = $this->entries->readResult($state, RedisProtocol::resultPayload($head));

        if ($result->served()) {
            return $result->withReason('result_overlay');
        }

        return $this->readCanonicalProjectionFallback(
            $context,
            $queryHash,
            $canonicalQueryHash,
            $this->canonicalHeadFrom($head, $version, RedisProtocol::MEMBERSHIP),
            $result->reason,
        );
    }

    /**
     * @param  array<int, mixed>  $head
     * @return array<int, mixed>
     */
    private function canonicalHeadFrom(
        array $head,
        string $version,
        string $status = RedisProtocol::HIT,
    ): array {
        $generation = RedisProtocol::resultGeneration($head);
        $membership = RedisProtocol::resultMembership($head);

        return is_string($membership)
            ? [$status, $version, $generation, $membership]
            : [RedisProtocol::MISS, $version, $generation];
    }

    private function readCanonicalProjectionFallback(
        ReadContext $context,
        string $queryHash,
        string $canonicalQueryHash,
        array $head,
        ?string $fallbackReason,
    ): CacheRead {
        $status = RedisProtocol::status($head);
        $version = RedisProtocol::version($head);

        if ($status === RedisProtocol::MEMBERSHIP || $status === RedisProtocol::HIT) {
            $generation = RedisProtocol::version($head, 2);
            $canonicalHead = [
                RedisProtocol::HIT,
                $version,
                $generation,
                RedisProtocol::canonicalPayload($head),
            ];
            $result = $this->readCanonicalHead(
                $context,
                $canonicalQueryHash,
                $canonicalHead,
                false,
            );

            if (!$result->served()) {
                $result = $this->revalidateProjectionSource(
                    $context,
                    $canonicalQueryHash,
                    $canonicalHead,
                    $result,
                );
            }

            if ($result->served()) {
                $projected = $this->projectRows(
                    $result->rows,
                    (array) $context->plan->projectedColumns,
                );

                if ($projected !== null) {
                    $result = $result->withRows($projected);
                    $promoted = $this->entries->promoteResult(
                        $context->query,
                        $context->plan,
                        $result->state,
                        $context->namespace,
                        $queryHash,
                        $projected,
                    );

                    return $fallbackReason === 'corrupt_payload'
                        ? $this->entries->rebuiltResultOutcome($result, $promoted)
                        : $result->withReason('canonical_projection_fallback');
                }
            }

            $fallbackReason = $result->reason ?? $fallbackReason;
        }

        $state = $this->states->resolve($context->plan, $context->namespace, $queryHash, $version);

        return new CacheRead($state, ReadOutcome::MISS, [], $fallbackReason);
    }

    /** @param array<int, mixed> $canonicalHead */
    private function revalidateProjectionSource(
        ReadContext $context,
        string $canonicalQueryHash,
        array $canonicalHead,
        CacheRead $stale,
    ): CacheRead {
        if (
            !$this->config->revalidation
            || !$this->revalidator->revalidate($context, $stale)
        ) {
            return $stale;
        }

        $revalidated = $this->readCanonicalHead(
            $context,
            $canonicalQueryHash,
            $canonicalHead,
            repairMissing: true,
            rootVersionApproved: true,
        );

        if (!$revalidated->served()) {
            return $stale;
        }

        $this->restamp($context, $revalidated->state, $revalidated->rows);

        return $revalidated;
    }

    /** @param list<mixed> $rows
     * @param  list<string>  $columns
     * @return list<\stdClass>|null
     */
    private function projectRows(array $rows, array $columns): ?array
    {
        $projectedRows = [];

        foreach ($rows as $row) {
            $projected = new \stdClass;

            foreach ($columns as $column) {
                if (!property_exists($row, $column)) {
                    return null;
                }

                $projected->{$column} = $row->{$column};
            }

            $projectedRows[] = $projected;
        }

        return $projectedRows;
    }

    private function readDirect(ReadContext $context, QueryHashResolver $hash): CacheRead
    {
        $cached = $this->rows->read($context->plan);

        $resolve = fn(): CacheState => $this->states->resolve(
            $context->plan,
            $context->namespace,
            $hash->value(),
            knownGeneration: $cached->generation,
        );

        if ($cached->row === null) {
            return new CacheRead($resolve(), ReadOutcome::MISS, [], $cached->reason);
        }

        $rows = $this->rows->visibleRows($context->plan, $cached->row);

        if ($rows === null) {
            return new CacheRead($resolve(), ReadOutcome::MISS, [], 'corrupt_payload');
        }

        return new CacheRead(
            $this->rows->state($context->plan, $cached->generation, (string) $cached->epoch),
            ReadOutcome::HIT,
            $rows,
        );
    }

    private function readResultRowFallback(QueryPlan $plan): ?CacheRead
    {
        $cached = $this->rows->read($plan);

        if ($cached->row === null) {
            return null;
        }

        $rows = $this->rows->visibleRows($plan, $cached->row);

        if ($rows === null) {
            return null;
        }

        $rows = $this->projectRows($rows, (array) $plan->projectedColumns);

        if ($rows === null) {
            return null;
        }

        return new CacheRead(
            $this->rows->state($plan, $cached->generation, (string) $cached->epoch),
            ReadOutcome::HIT,
            $rows,
            'row_cache_fallback',
        );
    }

    private function readCanonical(
        ReadContext $context,
        string $queryHash,
    ): CacheRead {
        $head = $this->store->fetchCanonical(
            versionKey: $this->keys->version($context->plan->root),
            generationKey: $this->keys->generation($context->plan->root),
            tablePrefix: $this->keys->tablePrefix($context->plan->root),
            namespace: $context->namespace,
            queryHash: $queryHash,
        );

        return $this->readCanonicalHead(
            $context,
            $queryHash,
            $head,
            true,
        );
    }

    private function readCanonicalHead(
        ReadContext $context,
        string $queryHash,
        array $head,
        bool $repairMissing,
        bool $rootVersionApproved = false,
    ): CacheRead {
        return $this->entries->readCanonical(
            $context->plan,
            $context->namespace,
            $queryHash,
            $head,
            $repairMissing,
            fn(CacheState $state, array $tokens): ?RowRepair => $this->repairer->repair(
                $context->query,
                $context->plan,
                $state,
                $tokens,
            ),
            $rootVersionApproved,
        );
    }

    private function revalidate(
        ReadContext $context,
        CacheRead $cached,
        string $queryHash,
        QueryStatement $statement,
    ): ?array {
        if (
            !$this->config->revalidation
            || !$this->revalidator->revalidate($context, $cached)
        ) {
            return null;
        }

        $revalidated = $this->readCanonicalHead(
            $context,
            $queryHash,
            [
                RedisProtocol::HIT,
                $cached->state->version,
                $cached->state->generation,
                $cached->staleMembershipRaw,
            ],
            repairMissing: true,
            rootVersionApproved: true,
        );

        if (!$revalidated->served()) {
            return null;
        }

        // Re-stamp against the state that validated the rows.
        $this->restamp($context, $revalidated->state, $revalidated->rows);
        $this->reportRead($context, $queryHash, $statement, $revalidated);

        return $revalidated->rows;
    }

    /** @param array<int, mixed> $rows */
    private function restamp(ReadContext $context, CacheState $state, array $rows): void
    {
        try {
            $this->entries->restampCanonical(
                $context->query,
                $context->plan,
                $state,
                $rows,
                $this->entries->inlineResult($state, $rows),
            );
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);
        }
    }

    /** @param array<int, mixed> $rows */
    private function publish(
        ReadContext $context,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        match (true) {
            $context->plan->isCanonical() => $this->publishCanonical(
                $context,
                $state,
                $rows,
                $lease,
            ),
            $context->plan->isDirectPrimaryKey() => $this->rows->publish($context->plan, $state, $rows, $lease),
            default => $this->entries->publishResult(
                $context->query,
                $context->plan,
                $state,
                $rows,
                $lease,
                $this->config->wakeTtl(),
            ),
        };
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
            $overlay->payload,
            $overlay->rejected,
        )) {
            $this->leases->release($lease);
        }
    }

    /** @param list<string> $dependencyHashes */
    private function canonicalQueryHash(
        ReadContext $context,
        Connection $connection,
        array $dependencyHashes,
        QueryStatement $statement,
    ): string {
        $query = $context->query;
        $preparedBindings = $statement->preparedBindings($connection);

        if ($query->columns === null || $query->columns === ['*']) {
            return $this->identity->hash(
                route: QueryPlan::CANONICAL,
                rootHash: $context->plan->root->hash,
                dependencyHashes: $dependencyHashes,
                sql: $statement->sql(),
                bindings: $preparedBindings,
                namespace: $context->namespace,
                operation: 'select',
            );
        }

        $canonical = $query->clone()->select('*');

        return $this->identity->hash(
            route: QueryPlan::CANONICAL,
            rootHash: $context->plan->root->hash,
            dependencyHashes: $dependencyHashes,
            sql: $canonical->toSql(),
            bindings: $query->bindings['select'] === []
                ? $preparedBindings
                : $connection->prepareBindings($canonical->getBindings()),
            namespace: $context->namespace,
            operation: 'select',
        );
    }
}
