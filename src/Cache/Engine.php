<?php

namespace NormCache\Cache;

use Illuminate\Database\Connection;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\CacheReadOutcome;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Support\Reporter;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\RuntimeState;
use NormCache\Values\TableIdentity;

final readonly class Engine
{
    public function __construct(
        private CacheConfig $config,
        private RuntimeState $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private TableIdentityResolver $tables,
        private PrimaryKeyResolver $primaryKeys,
        private QueryPlanner $planner,
        private QueryIdentity $identity,
        private RawResultCodec $codec,
        private DependencyAnalyzer $dependencies,
        private Reporter $reporter,
        private CacheSwitch $switch,
        private CacheStateResolver $states,
        private CanonicalRepository $canonical,
        private ResultRepository $results,
    ) {}

    /** @param list<mixed> $bindings
     * @param  callable(): array  $database
     * @param  callable(): array  $primaryDatabase
     */
    public function select(
        QueryBuilder $query,
        string $sql,
        array $bindings,
        string $operation,
        callable $database,
        callable $primaryDatabase,
    ): array {
        if (!$this->switch->readable()) {
            return $database();
        }

        $connection = $query->getConnection();
        $directRoot = $this->tables->resolve($connection, $query->from);
        $table = $directRoot ?? $this->declaredRoot($query, $connection);

        if ($table === null) {
            $this->reporter->bypass(
                $query,
                'unidentifiable_dependency',
                $sql,
                $bindings,
            );

            return $database();
        }

        if ($table->isView && $query->dependencies() === []) {
            $this->reporter->bypass(
                $query,
                'view_dependencies_required',
                $sql,
                $bindings,
            );

            return $database();
        }

        $analysis = $this->dependencies->analyze($connection, $query, $table);

        if ($analysis->volatile) {
            $this->reporter->bypass(
                $query,
                'volatile_expression',
                $sql,
                $bindings,
            );

            return $database();
        }

        if ($analysis->opaque && !$analysis->explicit) {
            $this->reporter->bypass(
                $query,
                'unidentifiable_dependency',
                $sql,
                $bindings,
            );

            return $database();
        }

        $dependencies = $analysis->tables;
        $primaryKey = $this->primaryKeys->resolve($query, $connection, $table);
        $plan = $this->planner->plan(
            $query,
            $table,
            $primaryKey,
            $dependencies,
            $analysis->opaque && $directRoot === null,
            $operation,
        );
        $namespace = $this->identity->namespace($query->configuredTag());
        $canonicalQueryHash = null;

        try {
            $dependencyHashes = array_map(
                static fn(TableIdentity $dependency): string => $dependency->hash,
                $dependencies,
            );

            if ($plan->route === QueryPlan::CANONICAL) {
                $queryHash = $canonicalQueryHash = $this->canonicalQueryHash(
                    $query,
                    $plan,
                    $connection,
                    $dependencyHashes,
                    $namespace,
                );
            } else {
                $queryHash = $this->identity->hash(
                    route: $plan->route,
                    rootHash: $table->hash,
                    dependencyHashes: $dependencyHashes,
                    sql: $sql,
                    bindings: $connection->prepareBindings($bindings),
                    namespace: $namespace,
                    operation: $operation,
                );
            }

            if (
                $plan->route === QueryPlan::RESULT
                    && $plan->projectedColumns !== null
                    && $plan->primaryKeyToken === null
            ) {
                $canonicalQueryHash = $this->canonicalQueryHash(
                    $query,
                    $plan,
                    $connection,
                    $dependencyHashes,
                    $namespace,
                );
            }
        } catch (\InvalidArgumentException) {
            $this->reporter->bypass(
                $query,
                'unsupported_query_shape',
                $sql,
                $bindings,
                $plan,
            );

            return $database();
        }

        try {
            [$state, $cached] = $this->read(
                $query,
                $plan,
                $namespace,
                $queryHash,
                $canonicalQueryHash,
            );

            if ($this->readOutcome($cached)->served()) {
                $this->reportRead($query, $plan, $queryHash, $sql, $bindings, $cached);

                return $cached['rows'];
            }
        } catch (\Throwable $exception) {
            $this->fail($exception);

            return $database();
        }

        try {
            $lease = $this->claim($plan, $state, $namespace, $queryHash);
        } catch (\Throwable $exception) {
            $this->fail($exception);

            return $database();
        }

        $this->reporter->miss(
            $query,
            $plan,
            $queryHash,
            $sql,
            $bindings,
            $cached['reason'] ?? null,
        );

        if (!$lease->owner) {
            if ($lease->wakeKey !== null) {
                try {
                    $this->store->brpop(
                        $lease->wakeKey,
                        $this->config->stampedeWaitMs / 1000,
                    );
                    [, $retry] = $this->read(
                        $query,
                        $plan,
                        $namespace,
                        $queryHash,
                        $canonicalQueryHash,
                    );

                    if ($this->readOutcome($retry)->served()) {
                        $this->reportRead($query, $plan, $queryHash, $sql, $bindings, $retry);

                        return $retry['rows'];
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
            $this->release($lease);

            throw $exception;
        }

        try {
            $after = $this->state($plan, $namespace, $queryHash);

            if ($after->equals($state)) {
                $this->publish(
                    $query,
                    $plan,
                    $state,
                    $rows,
                    $lease,
                    $namespace,
                    $queryHash,
                );
            } else {
                $this->release($lease);
            }
        } catch (\Throwable $exception) {
            $this->release($lease);
            $this->fail($exception);
        }

        return $rows;
    }

    /** @param array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome} $result */
    private function readOutcome(array $result): CacheReadOutcome
    {
        return $result['outcome'] ?? ($result['hit'] ? CacheReadOutcome::HIT : CacheReadOutcome::MISS);
    }

    /** @param array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome} $result */
    private function reportRead(
        QueryBuilder $query,
        QueryPlan $plan,
        string $queryHash,
        string $sql,
        array $bindings,
        array $result,
    ): void {
        $reason = $result['reason'] ?? null;

        if ($this->readOutcome($result) === CacheReadOutcome::REPAIRED) {
            $this->reporter->repaired($query, $plan, $queryHash, $sql, $bindings, $reason);

            return;
        }

        $this->reporter->hit($query, $plan, $queryHash, $sql, $bindings, $reason);
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}} */
    private function read(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        ?string $canonicalQueryHash = null,
    ): array {
        if ($plan->route === QueryPlan::CANONICAL) {
            return $plan->materializeResult
                ? $this->readCanonicalWithResultOverlay(
                    $query,
                    $plan,
                    $namespace,
                    $queryHash,
                )
                : $this->readCanonical($query, $plan, $namespace, $queryHash);
        }

        if ($plan->route === QueryPlan::DIRECT_PK) {
            return $this->readDirect($plan, $namespace, $queryHash);
        }

        if ($plan->route === QueryPlan::QUERY_GROUP) {
            $entryKey = $this->keys->queryGroupResult($queryHash, $namespace);
            [$state, $values] = $this->states->resolve(
                $plan,
                $namespace,
                $queryHash,
                alsoFetch: [$entryKey],
            );

            return [$state, $this->results->read($state, $values[$entryKey] ?? null)];
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

        return [$state, $this->results->read($state, $raw)];
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}} */
    private function readCanonicalWithResultOverlay(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
    ): array {
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
            $resultPlan = $this->resultOverlayPlan($plan);
            [$state] = $this->states->resolve($resultPlan, $namespace, $queryHash, $version);
            $result = $this->results->read($state, $head[2] ?? null);

            if ($this->readOutcome($result)->served()) {
                return [$state, $result];
            }

            $overlayReason = $result['reason'] ?? null;
            [$canonicalState, $canonicalResult] = $this->readCanonical(
                $query,
                $plan,
                $namespace,
                $queryHash,
            );

            if ($this->readOutcome($canonicalResult)->served()) {
                $promoted = $this->promoteResultPayload(
                    $query,
                    $resultPlan,
                    $canonicalState,
                    $namespace,
                    $queryHash,
                    $canonicalResult['rows'],
                    wakeWaiters: false,
                );

                if ($overlayReason === 'corrupt_payload') {
                    $canonicalResult = $this->withOverlayRebuildOutcome(
                        $canonicalResult,
                        $promoted,
                    );
                }
            }

            return [$canonicalState, $canonicalResult];
        }

        $generation = is_string($head[2] ?? null) ? $head[2] : '0';
        $canonicalHead = $status === 'membership'
            ? ['hit', $version, $generation, $head[3] ?? null]
            : [$status, $version, $generation];
        [$state, $result] = $this->readCanonicalHead(
            $query,
            $plan,
            $namespace,
            $queryHash,
            $canonicalHead,
            true,
        );

        if ($this->readOutcome($result)->served()) {
            $this->promoteResultPayload(
                $query,
                $this->resultOverlayPlan($plan),
                $state,
                $namespace,
                $queryHash,
                $result['rows'],
                wakeWaiters: false,
            );
        }

        return [$state, $result];
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}} */
    private function readResultOrCanonicalProjection(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        string $canonicalQueryHash,
    ): array {
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

            if ($this->readOutcome($result)->served()) {
                return [$state, $result];
            }

            $fallbackReason = $result['reason'] ?? null;
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
            $canonicalPlan = new QueryPlan(
                QueryPlan::CANONICAL,
                $plan->root,
                $plan->dependencies,
                $plan->primaryKey,
            );
            [$canonicalState, $result] = $this->readCanonicalHead(
                $query,
                $canonicalPlan,
                $namespace,
                $canonicalQueryHash,
                ['hit', $version, $generation, $head[3] ?? null],
                false,
            );

            if ($this->readOutcome($result)->served()) {
                $projected = $this->projectRows(
                    $result['rows'],
                    (array) $plan->projectedColumns,
                );

                if ($projected !== null) {
                    $result['rows'] = $projected;
                    $promoted = $this->promoteResultPayload(
                        $query,
                        $plan,
                        $canonicalState,
                        $namespace,
                        $queryHash,
                        $projected,
                    );

                    if ($fallbackReason === 'corrupt_payload') {
                        $result = $this->withOverlayRebuildOutcome($result, $promoted);
                    } else {
                        $result['reason'] = 'canonical_projection_fallback';
                    }

                    return [$canonicalState, $result];
                }
            }

            $fallbackReason = $result['reason'] ?? $fallbackReason;
        }

        [$state] = $this->states->resolve($plan, $namespace, $queryHash, $version);

        return [$state, ['hit' => false, 'rows' => [], 'reason' => $fallbackReason]];
    }

    /** @param array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome} $result
     * @return array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}
     */
    private function withOverlayRebuildOutcome(array $result, bool $promoted): array
    {
        $result['reason'] = $promoted
            ? 'result_overlay_rebuilt'
            : 'corrupt_result_overlay_fallback';

        if ($promoted) {
            $result['outcome'] = CacheReadOutcome::REPAIRED;
        }

        return $result;
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

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}} */
    private function readDirect(QueryPlan $plan, string $namespace, string $queryHash): array
    {
        $result = $this->store->fetchRow(
            $this->keys->generation($plan->root),
            $this->keys->tablePrefix($plan->root),
            (string) $plan->primaryKeyToken,
        );
        $generation = is_string($result[0] ?? null) ? $result[0] : '0';
        $raw = $result[1] ?? null;

        $resolve = fn(): CacheState => $this->states->resolve(
            $plan,
            $namespace,
            $queryHash,
            knownGeneration: $generation,
        )[0];

        if (!is_string($raw)) {
            return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => null]];
        }

        $payload = $this->codec->decodeRow(
            $raw,
            $plan->primaryKey,
            $plan->primaryKeyToken,
        );

        if (!$payload->valid) {
            return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => 'corrupt_payload']];
        }

        $epoch = $this->epoch();
        $hit = $payload->epoch === $epoch;
        $rows = $hit ? $payload->rows : [];

        if ($hit) {
            $visible = $this->applySoftDeleteVisibility($plan, $rows[0]);

            if ($visible === null) {
                return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => 'corrupt_payload']];
            }

            $rows = $visible;
        }

        if ($hit) {
            // version/versions are placeholders: select() returns on a hit and
            // never reads the state. Resolving them truthfully costs a round trip.
            return [
                new CacheState(
                    key: $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
                    epoch: $epoch,
                    version: '0',
                    generation: $generation,
                    versions: [],
                    tag: null,
                    tagKey: null,
                ),
                ['hit' => true, 'rows' => $rows, 'reason' => null],
            ];
        }

        return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => null]];
    }

    private function epoch(): string
    {
        return $this->switch->epoch();
    }

    /** @return list<\stdClass>|null null on missing deleted-at column; empty array when filtered by visibility. */
    private function applySoftDeleteVisibility(QueryPlan $plan, \stdClass $row): ?array
    {
        if ($plan->softDeleteMode === null || $plan->deletedAtColumn === null) {
            return [$row];
        }

        if (!property_exists($row, $plan->deletedAtColumn)) {
            return null;
        }

        $deleted = $row->{$plan->deletedAtColumn} !== null;

        if (
            $plan->softDeleteMode === 'default' && $deleted
            || $plan->softDeleteMode === 'only' && !$deleted
        ) {
            return [];
        }

        return [$row];
    }

    /** Reads the current canonical row after a result miss without changing the result-entry protocol. */
    private function readResultRowFallback(QueryPlan $plan): ?array
    {
        $result = $this->store->fetchRow(
            $this->keys->generation($plan->root),
            $this->keys->tablePrefix($plan->root),
            (string) $plan->primaryKeyToken,
        );
        $generation = is_string($result[0] ?? null) ? $result[0] : '0';
        $raw = $result[1] ?? null;

        if (!is_string($raw)) {
            return null;
        }

        $epoch = $this->epoch();
        $payload = $this->codec->decodeRow(
            $raw,
            $plan->primaryKey,
            $plan->primaryKeyToken,
        );

        if (!$payload->valid) {
            return null;
        }

        if ($payload->epoch !== $epoch) {
            return null;
        }

        $row = $payload->rows[0];

        $rows = $this->applySoftDeleteVisibility($plan, $row);

        if ($rows === null) {
            return null;
        }

        if ($rows !== []) {
            $projected = new \stdClass;

            foreach ((array) $plan->projectedColumns as $column) {
                if (!property_exists($row, $column)) {
                    return null;
                }

                $projected->{$column} = $row->{$column};
            }

            $rows = [$projected];
        }

        return [
            new CacheState(
                key: $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
                epoch: $epoch,
                version: '0',
                generation: $generation,
                versions: [],
                tag: null,
                tagKey: null,
            ),
            ['hit' => true, 'rows' => $rows, 'reason' => 'row_cache_fallback'],
        ];
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}} */
    private function readCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
    ): array {
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

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}} */
    private function readCanonicalHead(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $head,
        bool $repairMissing,
    ): array {
        return $this->canonical->read(
            $plan,
            $namespace,
            $queryHash,
            $head,
            $repairMissing,
            fn(CacheState $state, array $tokens): array => $this->repairRows(
                $query,
                $plan,
                $state,
                $tokens,
            ),
        );
    }

    /**
     * @param  list<string>  $tokens
     * @return array{rows: array<string, \stdClass>|null, outcome: CacheReadOutcome}
     */
    private function repairRows(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
    ): array {
        $tokens = array_values(array_unique($tokens));
        sort($tokens, SORT_STRING);

        $repairHash = $this->identity->repairHash(
            $plan->root->hash,
            $state->generation,
            $tokens,
        );
        $buildingKey = $this->keys->repairBuild($plan->root, $repairHash);
        $leaseToken = bin2hex(random_bytes(16));
        $wakeKey = $this->keys->repairWake($plan->root, $repairHash, $leaseToken);

        if (!$this->store->setNxEx(
            $buildingKey,
            $leaseToken,
            $this->config->buildingLockTtl,
        )) {
            $owner = $this->store->getRaw($buildingKey);

            if (is_string($owner)) {
                $this->store->brpop(
                    $this->keys->repairWake($plan->root, $repairHash, $owner),
                    $this->config->stampedeWaitMs / 1000,
                );
            }

            $rows = $this->readRepairedRows($plan, $state, $tokens);

            if ($rows === null || !$this->stateStillCurrent($plan, $state)) {
                return ['rows' => null, 'outcome' => CacheReadOutcome::MISS];
            }

            return [
                'rows' => $rows,
                'outcome' => CacheReadOutcome::HIT,
            ];
        }

        try {
            $rows = $this->buildRepairedRows(
                $query,
                $plan,
                $state,
                $tokens,
                $buildingKey,
                $wakeKey,
                $leaseToken,
            );
        } catch (\Throwable $exception) {
            $this->releaseRepair($buildingKey, $wakeKey, $leaseToken);

            throw $exception;
        }

        // Null means no publication script ran, so this caller still owns the lease.
        if ($rows === null) {
            $this->releaseRepair($buildingKey, $wakeKey, $leaseToken);
        }

        return ['rows' => $rows, 'outcome' => CacheReadOutcome::REPAIRED];
    }

    private function releaseRepair(string $buildingKey, string $wakeKey, string $token): void
    {
        $this->store->releaseBuilding($buildingKey, $wakeKey, $token, $this->wakeTtl());
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, \stdClass>|null
     */
    private function buildRepairedRows(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        string $buildingKey,
        string $wakeKey,
        string $leaseToken,
    ): ?array {
        $connection = $query->getConnection();
        $values = [];

        foreach ($tokens as $token) {
            $value = $plan->primaryKey->valueFromToken($token);

            if ($value === null) {
                return null;
            }

            $values[] = $value;
        }

        $limit = match ($plan->root->driver) {
            'sqlite' => 900,
            'sqlsrv' => 2000,
            default => 1000,
        };
        $rowsByToken = [];

        try {
            foreach (array_chunk($values, $limit) as $batch) {
                $rows = $connection
                    ->query()
                    ->from($plan->root->qualifiedTable())
                    ->whereIn($plan->primaryKey->column, $batch)
                    ->useWritePdo()
                    ->get();

                foreach ($rows as $row) {
                    if (!property_exists($row, $plan->primaryKey->column)) {
                        return null;
                    }

                    $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});

                    if ($token === null) {
                        return null;
                    }

                    $rowsByToken[$token] = $row;
                }
            }
        } catch (\Throwable) {
            return null;
        }

        if (!$this->stateStillCurrent($plan, $state)) {
            return null;
        }

        $rows = [];

        foreach ($tokens as $token) {
            if (!isset($rowsByToken[$token])) {
                return null;
            }

            $rowKey = $this->keys->row($plan->root, $state->generation, $token);
            $encoded = $this->codec->encodeRow($rowsByToken[$token], $state->epoch);
            $rows[$rowKey] = $encoded;
        }

        if (!$this->store->publishVersionedEntries(
            entries: $rows,
            ttl: $this->config->rowTtl,
            versionKeys: [
                $this->keys->version($plan->root),
                $this->keys->generation($plan->root),
            ],
            expectedVersions: [
                $state->version,
                $state->generation,
            ],
            buildingKey: $buildingKey,
            wakeKey: $wakeKey,
            token: $leaseToken,
            wakeTtl: $this->wakeTtl(),
        )) {
            return [];
        }

        return $this->stateStillCurrent($plan, $state) ? $rowsByToken : [];
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, \stdClass>|null
     */
    private function readRepairedRows(
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
    ): ?array {
        $rowKeys = [];

        foreach ($tokens as $token) {
            $rowKeys[$token] = $this->keys->row($plan->root, $state->generation, $token);
        }

        $raw = $this->states->fetch(array_values($rowKeys));
        $rows = [];

        foreach ($rowKeys as $token => $rowKey) {
            $payload = $raw[$rowKey] ?? null;
            $row = $payload === null
                ? null
                : $this->codec->decodeRowObject(
                    $payload,
                    $state->epoch,
                    $plan->primaryKey,
                    $token,
                );

            if ($row === null) {
                return null;
            }

            $rows[$token] = $row;
        }

        return $rows;
    }

    /** @phpstan-impure */
    private function stateStillCurrent(QueryPlan $plan, CacheState $state): bool
    {
        $epochKey = $this->keys->epoch();
        $versionKey = $this->keys->version($plan->root);
        $generationKey = $this->keys->generation($plan->root);
        $dependencyKeys = [];

        foreach ($plan->dependencies as $dependency) {
            if ($dependency->hash !== $plan->root->hash) {
                $dependencyKeys[$dependency->hash] = $this->keys->version($dependency);
            }
        }

        $values = $this->store->mget(array_values(array_filter([
            $epochKey,
            $versionKey,
            $generationKey,
            $state->tagKey,
            ...array_values($dependencyKeys),
        ])));
        $current = static fn(string $key): string => $values[$key] ?? '0';

        if (
            $current($epochKey) !== $state->epoch
            || $current($versionKey) !== $state->version
            || $current($generationKey) !== $state->generation
        ) {
            return false;
        }

        foreach ($dependencyKeys as $hash => $key) {
            if ($current($key) !== ($state->versions[$hash] ?? null)) {
                return false;
            }
        }

        return $state->tag === null
            || $state->tagKey !== null && $current($state->tagKey) === $state->tag;
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
            QueryPlan::DIRECT_PK => $this->publishDirect($plan, $state, $rows, $lease),
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
            $this->wakeTtl(),
        );
    }

    private function publishDirect(
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        if (
            count($rows) !== 1
            || !$rows[0] instanceof \stdClass
            || !property_exists($rows[0], $plan->primaryKey->column)
            || $plan->primaryKey->token($rows[0]->{$plan->primaryKey->column})
                !== $plan->primaryKeyToken
        ) {
            $this->release($lease);

            return;
        }

        $encoded = $this->codec->encodeRow($rows[0], $state->epoch);

        $this->store->publishVersionedEntries(
            entries: [$state->key => $encoded],
            ttl: $this->config->rowTtl,
            versionKeys: [
                $this->keys->version($plan->root),
                $this->keys->generation($plan->root),
            ],
            expectedVersions: [
                $state->version,
                $state->generation,
            ],
            buildingKey: $lease->buildingKey,
            wakeKey: $lease->wakeKey,
            token: $lease->token,
            wakeTtl: $this->wakeTtl(),
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
        if (!$this->canonical->publish(
            $query,
            $plan,
            $state,
            $rows,
            $lease,
            $this->wakeTtl(),
        )) {
            $this->release($lease);

            return;
        }

        if ($plan->materializeResult) {
            $this->promoteResultPayload(
                $query,
                $this->resultOverlayPlan($plan),
                $state,
                $namespace,
                $queryHash,
                $rows,
                wakeWaiters: false,
            );
        }
    }

    private function resultOverlayPlan(QueryPlan $plan): QueryPlan
    {
        return new QueryPlan(
            QueryPlan::RESULT,
            $plan->root,
            $plan->dependencies,
            $plan->primaryKey,
        );
    }

    /** @param array<int, mixed> $rows */
    private function promoteResultPayload(
        QueryBuilder $query,
        QueryPlan $resultPlan,
        CacheState $sourceState,
        string $namespace,
        string $queryHash,
        array $rows,
        bool $wakeWaiters = true,
    ): bool {
        try {
            $ttl = $query->configuredTtl() ?? $this->config->queryTtl;
            $encoded = $this->codec->encode(
                $rows,
                $sourceState->epoch,
                $sourceState->versions,
                $sourceState->tag,
            );

            $resultState = new CacheState(
                key: $this->keys->result(
                    $resultPlan->root,
                    $sourceState->version,
                    $namespace,
                    $queryHash,
                ),
                epoch: $sourceState->epoch,
                version: $sourceState->version,
                generation: '0',
                versions: $sourceState->versions,
                tag: $sourceState->tag,
                tagKey: $sourceState->tagKey,
            );
            $lease = $this->claim($resultPlan, $resultState, $namespace, $queryHash);

            if (!$lease->owner) {
                return false;
            }

            if (!$this->state($resultPlan, $namespace, $queryHash)->equals($resultState)) {
                $this->release($lease, $wakeWaiters);

                return false;
            }

            return $this->store->publishVersionedEntries(
                entries: [$resultState->key => $encoded],
                ttl: $ttl,
                versionKeys: [$this->keys->version($resultPlan->root)],
                expectedVersions: [$resultState->version],
                buildingKey: $lease->buildingKey,
                wakeKey: $wakeWaiters ? $lease->wakeKey : null,
                token: $lease->token,
                wakeTtl: $this->wakeTtl(),
            );
        } catch (\Throwable $exception) {
            if (isset($lease) && $lease->owner) {
                try {
                    $this->release($lease, $wakeWaiters);
                } catch (\Throwable) {
                    // The original Redis failure is the useful diagnostic.
                }
            }

            $this->fail($exception);

            return false;
        }
    }

    private function state(QueryPlan $plan, string $namespace, string $queryHash): CacheState
    {
        return $this->states->resolve($plan, $namespace, $queryHash)[0];
    }

    private function claim(
        QueryPlan $plan,
        CacheState $state,
        string $namespace,
        string $queryHash,
    ): BuildLease {
        $buildingKey = match ($plan->route) {
            QueryPlan::CANONICAL => $this->keys->membershipBuild(
                $plan->root,
                $state->version,
                $namespace,
                $queryHash,
            ),
            QueryPlan::RESULT => $this->keys->resultBuild(
                $plan->root,
                $state->version,
                $namespace,
                $queryHash,
            ),
            QueryPlan::DIRECT_PK => $this->keys->rowBuild(
                $plan->root,
                $state->generation,
                (string) $plan->primaryKeyToken,
            ),
            default => $this->keys->queryGroupBuild($queryHash),
        };
        $token = bin2hex(random_bytes(16));

        if ($this->store->setNxEx($buildingKey, $token, $this->config->buildingLockTtl)) {
            return new BuildLease(
                true,
                $buildingKey,
                $this->wakeKey($plan, $queryHash, $token),
                $token,
            );
        }

        $owner = $this->store->getRaw($buildingKey);

        return new BuildLease(
            false,
            $buildingKey,
            is_string($owner) ? $this->wakeKey($plan, $queryHash, $owner) : null,
            $owner,
        );
    }

    private function wakeKey(QueryPlan $plan, string $queryHash, string $token): string
    {
        return match ($plan->route) {
            QueryPlan::CANONICAL => $this->keys->wake($plan->root, 'm', $queryHash, $token),
            QueryPlan::RESULT => $this->keys->wake($plan->root, 'e', $queryHash, $token),
            QueryPlan::DIRECT_PK => $this->keys->wake(
                $plan->root,
                'r',
                (string) $plan->primaryKeyToken,
                $token,
            ),
            default => $this->keys->queryGroupWake($queryHash, $token),
        };
    }

    private function release(BuildLease $lease, bool $wakeWaiters = true): void
    {
        if (!$lease->owner || $lease->token === null) {
            return;
        }

        try {
            $this->store->releaseBuilding(
                $lease->buildingKey,
                $wakeWaiters ? (string) $lease->wakeKey : '',
                $lease->token,
                $this->wakeTtl(),
            );
        } catch (\Throwable $exception) {
            $this->fail($exception);
        }
    }

    private function wakeTtl(): int
    {
        return $this->config->buildingLockTtl
            + (int) ceil($this->config->stampedeWaitMs / 1000)
            + 5;
    }

    /** @param list<string> $dependencyHashes */
    private function canonicalQueryHash(
        QueryBuilder $query,
        QueryPlan $plan,
        Connection $connection,
        array $dependencyHashes,
        string $namespace,
    ): string {
        $canonical = $query->cloneWithoutBindings(['select']);
        $canonical->columns = ['*'];

        return $this->identity->hash(
            route: QueryPlan::CANONICAL,
            rootHash: $plan->root->hash,
            dependencyHashes: $dependencyHashes,
            sql: $canonical->toSql(),
            bindings: $connection->prepareBindings($canonical->getBindings()),
            namespace: $namespace,
            operation: 'select',
        );
    }

    private function declaredRoot(
        QueryBuilder $query,
        Connection $connection,
    ): ?TableIdentity {
        $resolved = [];

        foreach ($query->dependencies() as $declaration) {
            $identity = $declaration->isTable()
                ? $this->tables->resolve($connection, $declaration->value)
                : $this->dependencies->modelIdentity($connection, $declaration->value);

            if ($identity !== null) {
                $resolved[$identity->hash] = $identity;
            }
        }

        ksort($resolved, SORT_STRING);

        return array_values($resolved)[0] ?? null;
    }

    private function fail(\Throwable $exception): void
    {
        $this->runtime->fail($exception);
    }
}
