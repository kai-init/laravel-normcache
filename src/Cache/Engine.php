<?php

namespace NormCache\Cache;

use Illuminate\Database\Connection;
use InvalidArgumentException;
use NormCache\Database\CachingQueryBuilder;
use NormCache\Payload\MembershipCodec;
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
use stdClass;
use Throwable;

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
        private MembershipCodec $memberships,
        private DependencyAnalyzer $dependencies,
        private Reporter $reporter,
    ) {}

    /** @param list<mixed> $bindings
     * @param  callable(): array  $database
     * @param  callable(): array  $primaryDatabase
     */
    public function select(
        CachingQueryBuilder $query,
        string $sql,
        array $bindings,
        string $operation,
        callable $database,
        callable $primaryDatabase,
    ): array {
        if (!$this->config->enabled || !$this->runtime->available()) {
            return $database();
        }

        $connection = $query->getConnection();

        if (!$connection instanceof Connection) {
            return $database();
        }

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

        $analysis = $this->dependencies->analyze($connection, $query, $table);

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
        $namespace = $this->identity->namespace($query->normCacheTag());
        try {
            $queryHash = $this->identity->hash(
                route: $plan->route,
                rootHash: $table->hash,
                dependencyHashes: array_map(
                    static fn(TableIdentity $dependency): string => $dependency->hash,
                    $dependencies,
                ),
                sql: $sql,
                bindings: $connection->prepareBindings($bindings),
                namespace: $namespace,
                operation: $operation,
            );
        } catch (InvalidArgumentException) {
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
            [$state, $cached] = $this->read($query, $plan, $namespace, $queryHash);

            if ($cached['hit']) {
                if (($cached['reason'] ?? null) !== null) {
                    $this->reporter->miss(
                        $query,
                        $plan,
                        $queryHash,
                        $sql,
                        $bindings,
                        $cached['reason'],
                    );
                } else {
                    $this->reporter->hit($query, $plan, $queryHash, $sql, $bindings);
                }

                return $cached['rows'];
            }
        } catch (Throwable $exception) {
            $this->fail($exception);

            return $database();
        }

        try {
            $lease = $this->claim($plan, $state, $namespace, $queryHash);
        } catch (Throwable $exception) {
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
                    [, $retry] = $this->read($query, $plan, $namespace, $queryHash);

                    if ($retry['hit']) {
                        $this->reporter->hit($query, $plan, $queryHash, $sql, $bindings);

                        return $retry['rows'];
                    }
                } catch (Throwable $exception) {
                    $this->fail($exception);

                    return $database();
                }
            }

            return $primaryDatabase();
        }

        try {
            $rows = $primaryDatabase();
        } catch (Throwable $exception) {
            $this->release($lease);

            throw $exception;
        }

        try {
            $after = $this->state($plan, $namespace, $queryHash);

            if ($after->equals($state)) {
                $this->publish($query, $plan, $state, $rows, $lease);
            } else {
                $this->release($lease);
            }
        } catch (Throwable $exception) {
            $this->release($lease);
            $this->fail($exception);
        }

        return $rows;
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string}} */
    private function read(
        CachingQueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
    ): array {
        if ($plan->route === QueryPlan::CANONICAL) {
            return $this->readCanonical($query, $plan, $namespace, $queryHash);
        }

        if ($plan->route === QueryPlan::DIRECT_PK) {
            return $this->readDirect($plan, $namespace, $queryHash);
        }

        if ($plan->route === QueryPlan::QUERY_GROUP) {
            $entryKey = $this->keys->queryGroupResult($queryHash, $namespace);
            [$state, $values] = $this->resolveState(
                $plan,
                $namespace,
                $queryHash,
                alsoFetch: [$entryKey],
            );

            return [$state, $this->decodeResult($plan, $state, $values[$entryKey] ?? null)];
        }

        $entry = $this->store->fetchExact(
            $this->keys->version($plan->root),
            $this->keys->tablePrefix($plan->root),
            $namespace,
            $queryHash,
        );
        $version = is_string($entry[0] ?? null) ? $entry[0] : '0';
        [$state] = $this->resolveState($plan, $namespace, $queryHash, $version);

        return [$state, $this->decodeResult($plan, $state, $entry[1] ?? null)];
    }

    /** @return array{hit: bool, rows: array, reason: ?string} */
    private function decodeResult(QueryPlan $plan, CacheState $state, mixed $raw): array
    {
        if (!is_string($raw)) {
            return ['hit' => false, 'rows' => [], 'reason' => null];
        }

        $payload = $this->codec->decode($raw);

        if (!$payload->valid) {
            $this->store->delete($state->key);

            return ['hit' => false, 'rows' => [], 'reason' => 'corrupt_payload'];
        }

        $hit = $payload->epoch === $state->epoch
            && $payload->versions === $state->versions
            && $payload->tagVersion === $state->tag;

        return ['hit' => $hit, 'rows' => $hit ? $payload->rows : [], 'reason' => null];
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string}} */
    private function readDirect(QueryPlan $plan, string $namespace, string $queryHash): array
    {
        $result = $this->store->fetchRow(
            $this->keys->generation($plan->root),
            $this->keys->tablePrefix($plan->root),
            (string) $plan->primaryKeyToken,
        );
        $generation = is_string($result[0] ?? null) ? $result[0] : '0';
        $raw = $result[1] ?? null;

        $resolve = fn(): CacheState => $this->resolveState(
            $plan,
            $namespace,
            $queryHash,
            knownGeneration: $generation,
        )[0];

        if (!is_string($raw)) {
            return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => null]];
        }

        $payload = $this->codec->decodeRow($raw);

        if (!$payload->valid) {
            $this->store->delete(
                $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
            );

            return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => 'corrupt_payload']];
        }

        $epoch = $this->epoch();
        $hit = $payload->epoch === $epoch;
        $rows = $hit ? $payload->rows : [];

        if (
            $hit
            && $rows !== []
            && $plan->softDeleteMode !== null
            && $plan->deletedAtColumn !== null
        ) {
            $row = $rows[0];

            if (!property_exists($row, $plan->deletedAtColumn)) {
                $this->store->delete(
                    $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
                );

                return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => 'corrupt_payload']];
            }

            $deleted = $row->{$plan->deletedAtColumn} !== null;

            if (
                $plan->softDeleteMode === 'default' && $deleted
                || $plan->softDeleteMode === 'only' && !$deleted
            ) {
                $rows = [];
            }
        }

        if ($hit) {
            // version/versions/guard are placeholders: select() returns on a hit and
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
                    guard: '0',
                ),
                ['hit' => true, 'rows' => $rows, 'reason' => null],
            ];
        }

        return [$resolve(), ['hit' => false, 'rows' => [], 'reason' => null]];
    }

    private function epoch(): string
    {
        return $this->runtime->epoch(fn(): string => $this->store->getRaw($this->keys->epoch()) ?? '0');
    }

    /** @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string}} */
    private function readCanonical(
        CachingQueryBuilder $query,
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
            maxMembershipBytes: $this->config->maxMembershipBytes,
            maxMembershipRows: $this->config->maxMembershipRows,
        );
        $status = $head[0] ?? null;
        $version = is_string($head[1] ?? null) ? $head[1] : '0';
        $generation = is_string($head[2] ?? null) ? $head[2] : '0';
        $rawMembership = $status === 'hit' ? ($head[3] ?? null) : null;

        $miss = fn(?string $reason): array => [
            $this->resolveState($plan, $namespace, $queryHash, $version, $generation)[0],
            ['hit' => false, 'rows' => [], 'reason' => $reason],
        ];

        if (!is_string($rawMembership)) {
            return $miss($status === 'corrupt' ? 'corrupt_payload' : null);
        }

        $membership = $this->memberships->decode($rawMembership);

        if (!$membership->valid) {
            $this->store->delete(
                $this->keys->membership($plan->root, $version, $namespace, $queryHash),
            );

            return $miss('corrupt_payload');
        }

        $rowPrefix = $this->keys->tablePrefix($plan->root) . ':r:g' . $generation . ':';
        $unique = [];

        // §12 memberships may repeat a token, so index the reply by key, not position.
        foreach ($membership->ids as $token) {
            $unique[$rowPrefix . $token] = true;
        }

        [$state, $fetched] = $this->resolveState(
            $plan,
            $namespace,
            $queryHash,
            $version,
            $generation,
            array_keys($unique),
        );

        if (
            $membership->epoch !== $state->epoch
            || $membership->generation !== $state->generation
            || $membership->versions !== $state->versions
            || $membership->tagVersion !== $state->tag
            || count($membership->ids) > $this->config->maxMembershipRows
        ) {
            return [$state, ['hit' => false, 'rows' => [], 'reason' => null]];
        }

        if ($membership->ids === []) {
            return [$state, ['hit' => true, 'rows' => [], 'reason' => null]];
        }

        $rows = [];
        $missingAt = [];
        $corrupt = [];
        $bytes = 0;

        foreach ($membership->ids as $index => $token) {
            $rowKey = $rowPrefix . $token;
            $rawRow = $fetched[$rowKey] ?? null;

            if ($rawRow === null) {
                $missingAt[$index] = $token;

                continue;
            }

            $bytes += strlen($rawRow);

            if ($bytes > $this->config->maxCanonicalBytes) {
                return [$state, ['hit' => false, 'rows' => [], 'reason' => null]];
            }

            $rowObj = $this->codec->decodeRowObject($rawRow, $state->epoch);

            if ($rowObj === null) {
                $corrupt[] = $rowKey;
                $missingAt[$index] = $token;

                continue;
            }

            $rows[$index] = $rowObj;
        }

        if ($corrupt !== []) {
            $this->store->delete($corrupt);
        }

        if ($missingAt !== []) {
            $repaired = $this->repairRows($query, $plan, $state, array_values($missingAt));

            if ($repaired === null) {
                return [$state, ['hit' => false, 'rows' => [], 'reason' => null]];
            }

            foreach ($missingAt as $index => $token) {
                if (!isset($repaired[$token])) {
                    return [$state, ['hit' => false, 'rows' => [], 'reason' => null]];
                }

                $rows[$index] = $repaired[$token];
            }

            ksort($rows);
            $rows = array_values($rows);
        }

        return [$state, [
            'hit' => true,
            'rows' => $rows,
            'reason' => $corrupt !== [] ? 'corrupt_payload' : null,
        ]];
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, stdClass>|null
     */
    private function repairRows(
        CachingQueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
    ): ?array {
        if ($plan->primaryKey === null) {
            return null;
        }

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

            return $this->readRepairedRows($plan, $state, $tokens);
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
        } catch (Throwable $exception) {
            $this->releaseRepair($buildingKey, $wakeKey, $leaseToken);

            throw $exception;
        }

        // publish_repair.lua releases the lease itself on success; other exits still own it.
        if ($rows === null) {
            $this->releaseRepair($buildingKey, $wakeKey, $leaseToken);
        }

        return $rows;
    }

    private function releaseRepair(string $buildingKey, string $wakeKey, string $token): void
    {
        $this->store->releaseBuilding($buildingKey, $wakeKey, $token, $this->wakeTtl());
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, stdClass>|null
     */
    private function buildRepairedRows(
        CachingQueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        string $buildingKey,
        string $wakeKey,
        string $leaseToken,
    ): ?array {
        $connection = $query->getConnection();

        if ($plan->primaryKey === null || !$connection instanceof Connection) {
            return null;
        }

        $guardKeys = [];
        $values = [];

        foreach ($tokens as $token) {
            $value = $plan->primaryKey->valueFromToken($token);

            if ($value === null) {
                return null;
            }

            $guardKeys[] = $this->keys->guard($plan->root, $token);
            $values[] = $value;
        }

        $guards = [];

        foreach ($this->store->mget($guardKeys) as $guardKey => $guard) {
            $guards[$guardKey] = $guard ?? '0';
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
        } catch (Throwable) {
            return null;
        }

        $encodedRows = [];

        foreach ($tokens as $token) {
            if (!isset($rowsByToken[$token])) {
                return null;
            }

            $encodedRows[$this->keys->row($plan->root, $state->generation, $token)] =
                $this->codec->encodeRow($rowsByToken[$token], $state->epoch);
        }

        if (
            !$this->stateStillCurrent($plan, $state)
            || !$this->store->publishRepair(
                versionKey: $this->keys->version($plan->root),
                generationKey: $this->keys->generation($plan->root),
                guards: $guards,
                rows: $encodedRows,
                expectedVersion: $state->version,
                expectedGeneration: $state->generation,
                rowTtl: $this->config->ttl,
                buildingKey: $buildingKey,
                wakeKey: $wakeKey,
                token: $leaseToken,
                wakeTtl: $this->wakeTtl(),
            )
        ) {
            return null;
        }

        return $rowsByToken;
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, stdClass>|null
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

        $raw = $this->store->mget(array_values($rowKeys));
        $rows = [];

        foreach ($rowKeys as $token => $rowKey) {
            $payload = $raw[$rowKey] ?? null;
            $row = $payload === null
                ? null
                : $this->codec->decodeRowObject($payload, $state->epoch);

            if ($row === null) {
                return null;
            }

            $rows[$token] = $row;
        }

        return $rows;
    }

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
        CachingQueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        match ($plan->route) {
            QueryPlan::CANONICAL => $this->publishCanonical($query, $plan, $state, $rows, $lease),
            QueryPlan::DIRECT_PK => $this->publishDirect($plan, $state, $rows, $lease),
            default => $this->publishResult($query, $plan, $state, $rows, $lease),
        };
    }

    private function publishResult(
        CachingQueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        $encoded = $this->codec->encode(
            $rows,
            $state->epoch,
            $state->versions,
            $state->tag,
        );

        if (strlen($encoded) <= $this->config->maxResultBytes) {
            $versionKeys = $plan->route === QueryPlan::EXACT
                ? [$this->keys->version($plan->root)]
                : [];
            $expected = $plan->route === QueryPlan::EXACT
                ? [$state->version]
                : [];
            $this->store->storeVersionedPayload(
                entries: [$state->key => $encoded],
                ttl: $query->normCacheTtl() ?? $this->config->queryTtl,
                versionKeys: $versionKeys,
                expectedVersions: $expected,
                buildingKey: $lease->buildingKey,
                wakeKey: $lease->wakeKey,
                token: $lease->token,
                wakeTtl: $this->wakeTtl(),
            );
        } else {
            $this->release($lease);
        }
    }

    private function publishDirect(
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        if (
            $plan->primaryKey === null
            || count($rows) !== 1
            || !$rows[0] instanceof stdClass
            || !property_exists($rows[0], $plan->primaryKey->column)
            || $plan->primaryKey->token($rows[0]->{$plan->primaryKey->column})
                !== $plan->primaryKeyToken
        ) {
            $this->release($lease);

            return;
        }

        $encoded = $this->codec->encodeRow($rows[0], $state->epoch);

        if (strlen($encoded) <= $this->config->maxResultBytes) {
            $guardKey = $this->keys->guard(
                $plan->root,
                (string) $plan->primaryKeyToken,
            );
            $this->store->storeVersionedPayload(
                entries: [$state->key => $encoded],
                ttl: $this->config->ttl,
                versionKeys: [
                    $this->keys->version($plan->root),
                    $this->keys->generation($plan->root),
                    $guardKey,
                ],
                expectedVersions: [
                    $state->version,
                    $state->generation,
                    $state->guard,
                ],
                buildingKey: $lease->buildingKey,
                wakeKey: $lease->wakeKey,
                token: $lease->token,
                wakeTtl: $this->wakeTtl(),
            );
        } else {
            $this->release($lease);
        }
    }

    private function publishCanonical(
        CachingQueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
    ): void {
        if (
            $plan->primaryKey === null
            || count($rows) > $this->config->maxMembershipRows
        ) {
            $this->release($lease);

            return;
        }

        $ids = [];
        $encodedRows = [];
        $totalBytes = 0;

        foreach ($rows as $row) {
            if (!$row instanceof stdClass || !property_exists($row, $plan->primaryKey->column)) {
                $this->release($lease);

                return;
            }

            $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});

            if ($token === null) {
                $this->release($lease);

                return;
            }

            $encoded = $this->codec->encodeRow($row, $state->epoch);
            $totalBytes += strlen($encoded);

            if (
                strlen($encoded) > $this->config->maxResultBytes
                || $totalBytes > $this->config->maxCanonicalBytes
            ) {
                $this->release($lease);

                return;
            }

            $ids[] = $token;
            $encodedRows[$token] = $encoded;
        }

        $membership = $this->memberships->encode(
            epoch: $state->epoch,
            generation: $state->generation,
            ids: $ids,
            versions: $state->versions,
            tagVersion: $state->tag,
        );

        if (strlen($membership) > $this->config->maxMembershipBytes) {
            $this->release($lease);

            return;
        }

        $rowEntries = [];

        foreach ($encodedRows as $token => $encoded) {
            $rowEntries[$this->keys->row(
                $plan->root,
                $state->generation,
                $token,
            )] = $encoded;
        }

        $this->store->publishCanonical(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            membershipKey: $state->key,
            rows: $rowEntries,
            expectedVersion: $state->version,
            expectedGeneration: $state->generation,
            membershipPayload: $membership,
            membershipTtl: $query->normCacheTtl() ?? $this->config->queryTtl,
            rowTtl: $this->config->ttl,
            buildingKey: $lease->buildingKey,
            wakeKey: (string) $lease->wakeKey,
            token: (string) $lease->token,
            wakeTtl: $this->wakeTtl(),
        );
    }

    private function state(QueryPlan $plan, string $namespace, string $queryHash): CacheState
    {
        return $this->resolveState($plan, $namespace, $queryHash)[0];
    }

    /**
     * @param  list<string>  $alsoFetch
     * @return array{0: CacheState, 1: array<string, ?string>}
     */
    private function resolveState(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        ?string $knownVersion = null,
        ?string $knownGeneration = null,
        array $alsoFetch = [],
    ): array {
        $versionKeys = [];

        foreach ($plan->dependencies as $dependency) {
            $versionKeys[$dependency->hash] = $this->keys->version($dependency);
        }

        $rootVersionKey = $versionKeys[$plan->root->hash] ?? null;

        if ($knownVersion !== null && $rootVersionKey !== null) {
            unset($versionKeys[$plan->root->hash]);
        }

        $generationKey = $knownGeneration === null ? match ($plan->route) {
            QueryPlan::CANONICAL, QueryPlan::DIRECT_PK => $this->keys->generation($plan->root),
            default => null,
        } : null;
        $tagKey = str_starts_with($namespace, 'g')
            ? $this->keys->tagVersion(substr($namespace, 1))
            : null;
        $guardKey = $plan->route === QueryPlan::DIRECT_PK
            ? $this->keys->guard($plan->root, (string) $plan->primaryKeyToken)
            : null;

        // Batched, not fetched on its own: a standalone GET made a scope's first
        // canonical read 3 round trips instead of 2.
        $epochKey = $this->runtime->knownEpoch() === null ? $this->keys->epoch() : null;

        $values = $this->store->mget(array_values(array_unique(array_filter([
            $epochKey,
            ...array_values($versionKeys),
            $generationKey,
            $tagKey,
            $guardKey,
            ...$alsoFetch,
        ]))));

        if ($epochKey !== null) {
            $this->runtime->rememberEpoch($values[$epochKey] ?? '0');
        }

        if ($knownVersion !== null && $rootVersionKey !== null) {
            $versionKeys[$plan->root->hash] = $rootVersionKey;
            $values[$rootVersionKey] = $knownVersion;
        }

        $epoch = $this->epoch();
        $allVersions = [];

        foreach ($versionKeys as $hash => $key) {
            $allVersions[$hash] = $values[$key] ?? '0';
        }

        ksort($allVersions, SORT_STRING);
        $rootVersion = $knownVersion ?? $allVersions[$plan->root->hash] ?? '0';
        $generation = $knownGeneration
            ?? ($generationKey !== null ? ($values[$generationKey] ?? '0') : '0');
        $tag = $tagKey !== null ? ($values[$tagKey] ?? '0') : null;
        $guard = $guardKey !== null ? ($values[$guardKey] ?? '0') : '0';
        $versions = $allVersions;

        if ($plan->route !== QueryPlan::QUERY_GROUP) {
            unset($versions[$plan->root->hash]);
        }

        $key = match ($plan->route) {
            QueryPlan::CANONICAL => $this->keys->membership(
                $plan->root,
                $rootVersion,
                $namespace,
                $queryHash,
            ),
            QueryPlan::DIRECT_PK => $this->keys->row(
                $plan->root,
                $generation,
                (string) $plan->primaryKeyToken,
            ),
            QueryPlan::QUERY_GROUP => $this->keys->queryGroupResult($queryHash, $namespace),
            default => $this->keys->exact(
                $plan->root,
                $rootVersion,
                $namespace,
                $queryHash,
            ),
        };

        return [
            new CacheState(
                key: $key,
                epoch: $epoch,
                version: $rootVersion,
                generation: $generation,
                versions: $versions,
                tag: $tag,
                tagKey: $tagKey,
                guard: $guard,
            ),
            $values,
        ];
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
            QueryPlan::EXACT => $this->keys->exactBuild(
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
            QueryPlan::EXACT => $this->keys->wake($plan->root, 'e', $queryHash, $token),
            QueryPlan::DIRECT_PK => $this->keys->wake(
                $plan->root,
                'r',
                (string) $plan->primaryKeyToken,
                $token,
            ),
            default => $this->keys->queryGroupWake($queryHash, $token),
        };
    }

    private function release(BuildLease $lease): void
    {
        if (!$lease->owner || $lease->token === null || $lease->wakeKey === null) {
            return;
        }

        try {
            $this->store->releaseBuilding(
                $lease->buildingKey,
                $lease->wakeKey,
                $lease->token,
                $this->wakeTtl(),
            );
        } catch (Throwable $exception) {
            $this->fail($exception);
        }
    }

    private function wakeTtl(): int
    {
        return $this->config->buildingLockTtl
            + (int) ceil($this->config->stampedeWaitMs / 1000)
            + 5;
    }

    private function declaredRoot(
        CachingQueryBuilder $query,
        Connection $connection,
    ): ?TableIdentity {
        foreach ($query->normCacheDeclaredTables() as $table) {
            $identity = $this->tables->resolve($connection, $table);

            if ($identity !== null) {
                return $identity;
            }
        }

        foreach ($query->normCacheDeclaredModels() as $modelClass) {
            $identity = $this->dependencies->modelIdentity($connection, $modelClass);

            if ($identity !== null) {
                return $identity;
            }
        }

        return null;
    }

    private function fail(Throwable $exception): void
    {
        $this->runtime->fail($exception);
    }
}
