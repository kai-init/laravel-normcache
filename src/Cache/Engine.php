<?php

namespace NormCache\Cache;

use Illuminate\Database\Connection;
use NormCache\Database\QueryBuilder;
use NormCache\Database\QueryStatement;
use NormCache\Enums\ReadOutcome;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\QueryIdentity;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\PrimaryKeyMetadata;
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
        private TableIdentityResolver $tables,
        private PrimaryKeyResolver $primaryKeys,
        private QueryPlanner $planner,
        private QueryIdentity $identity,
        private DependencyAnalyzer $dependencies,
        private QueryObserver $observer,
        private CacheStateResolver $states,
        private CanonicalRepository $canonical,
        private ResultRepository $results,
        private BuildLeaseCoordinator $leases,
        private RowRepairer $repairer,
        private ResultOverlayPublisher $overlays,
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
        $directRoot = $this->tables->resolve($connection, $query->from);
        $table = $directRoot ?? $this->declaredRoot($query, $connection);

        if ($table === null) {
            return $this->bypass(
                $query,
                'unidentifiable_dependency',
                $statement,
                $database,
            );
        }

        if ($table->isView && $query->dependencies() === []) {
            return $this->bypass(
                $query,
                'view_dependencies_required',
                $statement,
                $database,
            );
        }

        $analysis = $this->dependencies->analyze($connection, $query, $table);

        if ($analysis->volatile) {
            return $this->bypass(
                $query,
                'volatile_expression',
                $statement,
                $database,
            );
        }

        if ($analysis->unresolved) {
            return $this->bypass(
                $query,
                'unresolvable_declared_dependency',
                $statement,
                $database,
            );
        }

        if ($analysis->opaque && !$analysis->explicit) {
            return $this->bypass(
                $query,
                'unidentifiable_dependency',
                $statement,
                $database,
            );
        }

        $dependencies = $analysis->tables;
        $forceQueryGroup = $analysis->opaque && $directRoot === null;
        $plan = $this->planner->plan(
            $query,
            $table,
            fn(): ?PrimaryKeyMetadata => $this->primaryKeys->resolve($query, $connection, $table),
            $dependencies,
            $forceQueryGroup,
            $operation,
        );
        $namespace = $this->identity->namespace($query->configuredTag());
        $canonicalQueryHash = null;
        $dependencyHashes = array_map(
            static fn(TableIdentity $dependency): string => $dependency->hash,
            $dependencies,
        );
        $hash = $this->queryHashResolver(
            $query,
            $plan,
            $connection,
            $dependencyHashes,
            $namespace,
            $operation,
            $statement,
        );

        try {
            $canonicalQueryHash = $this->resultOverlayCanonicalHash(
                $query,
                $plan,
                $connection,
                $dependencyHashes,
                $namespace,
                $statement,
            );
            $cached = $this->readCache(
                $query,
                $plan,
                $namespace,
                $hash,
                $canonicalQueryHash,
            );

            if ($cached->served()) {
                if ($this->observer->observing()) {
                    $this->reportRead(
                        $query,
                        $plan,
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
            $this->fail($exception);

            return $database();
        }

        try {
            $queryHash = $hash->value();
            $lease = $this->leases->claim($plan, $cached->state, $namespace, $queryHash);
        } catch (\Throwable $exception) {
            $this->fail($exception);

            return $database();
        }

        $this->observer->miss(
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
                    $retry = $this->read(
                        $query,
                        $plan,
                        $namespace,
                        $queryHash,
                        $canonicalQueryHash,
                    );

                    if ($retry->served()) {
                        $this->reportRead(
                            $query,
                            $plan,
                            $queryHash,
                            $statement,
                            $retry,
                        );

                        return $retry->rows;
                    }
                } catch (\Throwable $exception) {
                    $this->fail($exception);

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
            $after = $this->state($plan, $namespace, $queryHash);

            if ($after->equals($cached->state)) {
                $this->publish(
                    $query,
                    $plan,
                    $cached->state,
                    $rows,
                    $lease,
                    $namespace,
                    $queryHash,
                );
            } else {
                $this->leases->release($lease);
            }
        } catch (\Throwable $exception) {
            $this->leases->release($lease);
            $this->fail($exception);
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
        QueryBuilder $query,
        QueryPlan $plan,
        Connection $connection,
        array $dependencyHashes,
        string $namespace,
        string $operation,
        QueryStatement $statement,
    ): QueryHashResolver {
        if ($plan->route === QueryPlan::CANONICAL) {
            return new QueryHashResolver(fn(): string => $this->canonicalQueryHash(
                $query,
                $plan,
                $connection,
                $dependencyHashes,
                $namespace,
                $statement,
            ));
        }

        return new QueryHashResolver(function () use (
            $plan,
            $dependencyHashes,
            $namespace,
            $operation,
            $statement,
            $connection,
        ): string {
            return $this->identity->hash(
                route: $plan->route,
                rootHash: $plan->root->hash,
                dependencyHashes: $dependencyHashes,
                sql: $statement->sql(),
                bindings: $statement->preparedBindings($connection),
                namespace: $namespace,
                operation: $operation,
            );
        });
    }

    /** @param list<string> $dependencyHashes */
    private function resultOverlayCanonicalHash(
        QueryBuilder $query,
        QueryPlan $plan,
        Connection $connection,
        array $dependencyHashes,
        string $namespace,
        QueryStatement $statement,
    ): ?string {
        if (
            $plan->route !== QueryPlan::RESULT
            || $plan->projectedColumns === null
            || $plan->primaryKeyToken !== null
        ) {
            return null;
        }

        return $this->canonicalQueryHash(
            $query,
            $plan,
            $connection,
            $dependencyHashes,
            $namespace,
            $statement,
        );
    }

    private function readCache(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        QueryHashResolver $hash,
        ?string $canonicalQueryHash,
    ): CacheRead {
        if ($plan->route === QueryPlan::DIRECT_PK) {
            return $this->readDirect($plan, $namespace, $hash);
        }

        return $this->read(
            $query,
            $plan,
            $namespace,
            $hash->value(),
            $canonicalQueryHash,
        );
    }

    private function reportRead(
        QueryBuilder $query,
        QueryPlan $plan,
        string $queryHash,
        QueryStatement $statement,
        CacheRead $read,
    ): void {
        if ($read->outcome === ReadOutcome::REPAIRED) {
            $this->observer->repaired(
                $query,
                $plan,
                $queryHash,
                $statement,
                $read->reason,
            );

            return;
        }

        $this->observer->hit(
            $query,
            $plan,
            $queryHash,
            $statement,
            $read->reason,
        );
    }

    private function read(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        ?string $canonicalQueryHash = null,
    ): CacheRead {
        if ($plan->route === QueryPlan::CANONICAL) {
            return $plan->materializeResult && $this->config->maxAutoOverlayRows > 0
                ? $this->readCanonicalWithResultOverlay(
                    $query,
                    $plan,
                    $namespace,
                    $queryHash,
                )
                : $this->readCanonical($query, $plan, $namespace, $queryHash);
        }

        if ($plan->route === QueryPlan::QUERY_GROUP) {
            $entryKey = $this->keys->queryGroupResult($queryHash, $namespace);
            [$state, $values] = $this->states->resolve(
                $plan,
                $namespace,
                $queryHash,
                alsoFetch: [$entryKey],
            );

            return $this->results->read($state, $values[$entryKey] ?? null);
        }

        if (
            $canonicalQueryHash !== null
            && $plan->projectedColumns !== null
            && $plan->primaryKeyToken === null
        ) {
            return $this->readResultOrCanonicalProjection(
                $query,
                $plan,
                $namespace,
                $queryHash,
                $canonicalQueryHash,
            );
        }

        $entry = $this->store->fetchResult(
            $this->keys->version($plan->root),
            $this->keys->tablePrefix($plan->root),
            $namespace,
            $queryHash,
        );
        $version = is_string($entry[0] ?? null) ? $entry[0] : '0';
        $raw = $entry[1] ?? null;

        if (
            !is_string($raw)
            && $plan->projectedColumns !== null
            && $plan->primaryKeyToken !== null
        ) {
            $fallback = $this->readResultRowFallback($plan);

            if ($fallback !== null) {
                return $fallback;
            }
        }

        [$state] = $this->states->resolve($plan, $namespace, $queryHash, $version);

        return $this->results->read($state, $raw);
    }

    private function readCanonicalWithResultOverlay(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
    ): CacheRead {
        $head = $this->store->fetchResultOrCanonical(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            tablePrefix: $this->keys->tablePrefix($plan->root),
            namespace: $namespace,
            resultQueryHash: $queryHash,
            canonicalQueryHash: $queryHash,
        );
        $status = $head[0] ?? null;
        $version = is_string($head[1] ?? null) ? $head[1] : '0';

        if ($status === 'result') {
            $resultPlan = $plan->asFullResultOverlay();
            [$state] = $this->states->resolve($resultPlan, $namespace, $queryHash, $version);
            $result = $this->results->read($state, $head[2] ?? null);

            if ($result->served()) {
                return $result->withReason('result_overlay');
            }

            $overlayReason = $result->reason;
            $canonicalResult = $this->readCanonical(
                $query,
                $plan,
                $namespace,
                $queryHash,
            );

            if ($canonicalResult->served()) {
                $promoted = $this->overlays->promote(
                    $query,
                    $resultPlan,
                    $canonicalResult->state,
                    $namespace,
                    $queryHash,
                    $canonicalResult->rows,
                    wakeWaiters: false,
                );

                if ($overlayReason === 'corrupt_payload') {
                    $canonicalResult = $this->overlays->rebuildOutcome(
                        $canonicalResult,
                        $promoted,
                    );
                }
            }

            return $canonicalResult;
        }

        $generation = is_string($head[2] ?? null) ? $head[2] : '0';
        $canonicalHead = $status === 'membership'
            ? ['hit', $version, $generation, $head[3] ?? null]
            : [$status, $version, $generation];
        $result = $this->readCanonicalHead(
            $query,
            $plan,
            $namespace,
            $queryHash,
            $canonicalHead,
            true,
        );

        if ($result->served()) {
            $this->overlays->promote(
                $query,
                $plan->asFullResultOverlay(),
                $result->state,
                $namespace,
                $queryHash,
                $result->rows,
                wakeWaiters: false,
            );
        }

        return $result;
    }

    private function readResultOrCanonicalProjection(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        string $canonicalQueryHash,
    ): CacheRead {
        $head = $this->store->fetchResultOrCanonical(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            tablePrefix: $this->keys->tablePrefix($plan->root),
            namespace: $namespace,
            resultQueryHash: $queryHash,
            canonicalQueryHash: $canonicalQueryHash,
        );
        $status = $head[0] ?? null;
        $version = is_string($head[1] ?? null) ? $head[1] : '0';
        $fallbackReason = $status === 'corrupt' ? 'corrupt_payload' : null;

        if ($status === 'result') {
            [$state] = $this->states->resolve($plan, $namespace, $queryHash, $version);
            $result = $this->results->read($state, $head[2] ?? null);

            if ($result->served()) {
                return $result->withReason('result_overlay');
            }

            $fallbackReason = $result->reason;
            $head = $this->store->fetchCanonical(
                versionKey: $this->keys->version($plan->root),
                generationKey: $this->keys->generation($plan->root),
                tablePrefix: $this->keys->tablePrefix($plan->root),
                namespace: $namespace,
                queryHash: $canonicalQueryHash,
            );
            $status = $head[0] ?? null;
            $version = is_string($head[1] ?? null) ? $head[1] : '0';
        }

        if ($status === 'membership' || $status === 'hit') {
            $generation = is_string($head[2] ?? null) ? $head[2] : '0';
            $result = $this->readCanonicalHead(
                $query,
                $plan->asCanonicalProjectionFallback(),
                $namespace,
                $canonicalQueryHash,
                ['hit', $version, $generation, $head[3] ?? null],
                false,
            );

            if ($result->served()) {
                $projected = $this->projectRows(
                    $result->rows,
                    (array) $plan->projectedColumns,
                );

                if ($projected !== null) {
                    $result = $result->withRows($projected);
                    $promoted = $this->overlays->promote(
                        $query,
                        $plan,
                        $result->state,
                        $namespace,
                        $queryHash,
                        $projected,
                    );

                    return $fallbackReason === 'corrupt_payload'
                        ? $this->overlays->rebuildOutcome($result, $promoted)
                        : $result->withReason('canonical_projection_fallback');
                }
            }

            $fallbackReason = $result->reason ?? $fallbackReason;
        }

        [$state] = $this->states->resolve($plan, $namespace, $queryHash, $version);

        return new CacheRead($state, ReadOutcome::MISS, [], $fallbackReason);
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

    private function readDirect(QueryPlan $plan, string $namespace, QueryHashResolver $hash): CacheRead
    {
        $cached = $this->rows->read($plan);

        $resolve = fn(): CacheState => $this->states->resolve(
            $plan,
            $namespace,
            $hash->value(),
            knownGeneration: $cached->generation,
        )[0];

        if ($cached->row === null) {
            return new CacheRead($resolve(), ReadOutcome::MISS, [], $cached->reason);
        }

        $rows = $this->rows->visibleRows($plan, $cached->row);

        if ($rows === null) {
            return new CacheRead($resolve(), ReadOutcome::MISS, [], 'corrupt_payload');
        }

        return new CacheRead(
            $this->rows->state($plan, $cached->generation, (string) $cached->epoch),
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
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
    ): CacheRead {
        $head = $this->store->fetchCanonical(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            tablePrefix: $this->keys->tablePrefix($plan->root),
            namespace: $namespace,
            queryHash: $queryHash,
        );

        return $this->readCanonicalHead(
            $query,
            $plan,
            $namespace,
            $queryHash,
            $head,
            true,
        );
    }

    private function readCanonicalHead(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $head,
        bool $repairMissing,
    ): CacheRead {
        return $this->canonical->read(
            $plan,
            $namespace,
            $queryHash,
            $head,
            $repairMissing,
            fn(CacheState $state, array $tokens): ?RowRepair => $this->repairer->repair(
                $query,
                $plan,
                $state,
                $tokens,
            ),
        );
    }

    /** @param array<int, mixed> $rows */
    private function publish(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        string $namespace,
        string $queryHash,
    ): void {
        match ($plan->route) {
            QueryPlan::CANONICAL => $this->publishCanonical(
                $query,
                $plan,
                $state,
                $rows,
                $lease,
                $namespace,
                $queryHash,
            ),
            QueryPlan::DIRECT_PK => $this->rows->publish($plan, $state, $rows, $lease),
            default => $this->publishResult($query, $plan, $state, $rows, $lease),
        };
    }

    private function publishResult(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        $this->results->publish(
            $query,
            $plan,
            $state,
            $rows,
            $lease,
            $this->config->wakeTtl(),
        );
    }

    private function publishCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        string $namespace,
        string $queryHash,
    ): void {
        $overlay = $plan->materializeResult
            ? $this->overlays->inlineEntry(
                $plan->root,
                $state,
                $namespace,
                $queryHash,
                $rows,
            )
            : null;

        if (!$this->canonical->publish(
            $query,
            $plan,
            $state,
            $rows,
            $lease,
            $this->config->wakeTtl(),
            $overlay,
        )) {
            $this->leases->release($lease);
        }
    }

    private function state(QueryPlan $plan, string $namespace, string $queryHash): CacheState
    {
        return $this->states->resolve($plan, $namespace, $queryHash)[0];
    }

    /** @param list<string> $dependencyHashes */
    private function canonicalQueryHash(
        QueryBuilder $query,
        QueryPlan $plan,
        Connection $connection,
        array $dependencyHashes,
        string $namespace,
        QueryStatement $statement,
    ): string {
        if ($query->columns === null || $query->columns === ['*']) {
            return $this->identity->hash(
                route: QueryPlan::CANONICAL,
                rootHash: $plan->root->hash,
                dependencyHashes: $dependencyHashes,
                sql: $statement->sql(),
                bindings: $statement->preparedBindings($connection),
                namespace: $namespace,
                operation: 'select',
            );
        }

        $canonical = $query->cloneWithoutBindings(['select']);
        $canonical->columns = ['*'];

        return $this->identity->hash(
            route: QueryPlan::CANONICAL,
            rootHash: $plan->root->hash,
            dependencyHashes: $dependencyHashes,
            sql: $canonical->toSql(),
            bindings: $query->bindings['select'] === []
                ? $statement->preparedBindings($connection)
                 : $connection->prepareBindings($canonical->getBindings()),
            namespace: $namespace,
            operation: 'select',
        );
    }

    private function declaredRoot(
        QueryBuilder $query,
        Connection $connection,
    ): ?TableIdentity {
        $lowest = null;

        foreach ($query->dependencies() as $declaration) {
            $identity = $declaration->isTable()
                ? $this->tables->resolve($connection, $declaration->value)
                : $this->dependencies->modelIdentity($connection, $declaration->value);

            if ($identity !== null && ($lowest === null || $identity->hash < $lowest->hash)) {
                $lowest = $identity;
            }
        }

        return $lowest;
    }

    private function fail(\Throwable $exception): void
    {
        $this->runtime->fail($exception);
    }
}
