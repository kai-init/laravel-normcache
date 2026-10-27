<?php

namespace NormCache\Cache;

use Illuminate\Contracts\Container\Container;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\QueryPlanner;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisProtocol;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\LoadedCanonical;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableQueryReply;

final readonly class QueryEntryRepository
{
    private const MAX_AUTO_OVERLAY_BYTES = 128 * 1024;

    private const EAGER_AUTO_OVERLAY_BYTES = 32 * 1024;

    private const PAGINATION_LOOKAHEAD_ROWS = 1;

    public function __construct(
        private CacheConfig $config,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private CacheStateResolver $states,
        private RawResultCodec $codec,
        private MembershipCodec $memberships,
        private Container $container,
    ) {}

    public function read(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        bool $claim = false,
    ): CacheRead {
        if ($plan->isCanonical()) {
            $claimToken = $claim ? bin2hex(random_bytes(16)) : null;
            $reply = $this->store->fetchTableQuery(
                $this->keys->version($plan->root),
                $this->keys->generation($plan->root),
                $this->keys->tablePrefix($plan->root),
                $namespace,
                $queryHash,
                $this->config->maxAutoOverlayRows > 0 ? 'prefer-result' : 'membership',
                $claimToken,
                $claimToken === null ? 0 : $this->config->buildingLockTtl,
            );
            $entry = RedisProtocol::decodeTableQuery($reply);

            if ($entry->kind === RedisProtocol::RESULT) {
                $state = $this->states->resolve(
                    $plan,
                    $namespace,
                    $queryHash,
                    $entry->version,
                    usesGeneration: false,
                );
                $result = $this->readResult($state, $entry->payload);

                return $result->served()
                    ? $result->withReason('result_overlay')
                    : new CacheRead(
                        $this->states->resolve($plan, $namespace, $queryHash),
                        ReadOutcome::MISS,
                        [],
                        $result->reason,
                    );
            }

            return $this->readCanonical($query, $plan, $namespace, $queryHash, $entry);
        }

        if ($plan->isQueryGroup()) {
            [$raw, $values] = $this->store->readHashFieldWithValues(
                $this->keys->queryGroupEntry($queryHash, $namespace),
                'r',
                $this->states->pendingStateKeys($plan, $namespace),
            );
            $state = $this->states->resolve(
                $plan,
                $namespace,
                $queryHash,
                prefetched: $values,
            );
        } else {
            $claimToken = $claim ? bin2hex(random_bytes(16)) : null;
            $reply = $this->store->fetchTableQuery(
                $this->keys->version($plan->root),
                $this->keys->generation($plan->root),
                $this->keys->tablePrefix($plan->root),
                $namespace,
                $queryHash,
                'result',
                $claimToken,
                $claimToken === null ? 0 : $this->config->buildingLockTtl,
            );
            $entry = RedisProtocol::decodeTableQuery($reply);
            $version = $entry->version;
            $lease = $this->queryBuildLease($plan, $namespace, $queryHash, $entry);
            try {
                $state = $this->states->resolve(
                    $plan,
                    $namespace,
                    $queryHash,
                    $version,
                    usesGeneration: false,
                );
                $result = $this->readResult($state, $entry->payload);
            } catch (\Throwable $exception) {
                if ($lease !== null) {
                    $this->container->make(BuildLeaseCoordinator::class)->release($lease);
                }

                throw $exception;
            }

            return $lease === null
                ? $result
                : new CacheRead($result->state, $result->outcome, $result->rows, $result->reason, $lease);
        }

        return $this->readResult($state, $raw);
    }

    public function readCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        TableQueryReply $entry,
    ): CacheRead {
        $version = $entry->version;
        $generation = $entry->generation ?? '0';
        $lease = $this->queryBuildLease($plan, $namespace, $queryHash, $entry);
        $miss = function (?string $reason) use ($plan, $namespace, $queryHash, $version, $generation, $lease): CacheRead {
            try {
                $state = $this->states->resolve($plan, $namespace, $queryHash, $version, $generation);
            } catch (\Throwable $exception) {
                if ($lease !== null) {
                    $this->container->make(BuildLeaseCoordinator::class)->release($lease);
                }

                throw $exception;
            }

            return new CacheRead($state, ReadOutcome::MISS, [], $reason, $lease);
        };

        if ($entry->kind === RedisProtocol::MISS || $entry->payload === null) {
            return $miss(null);
        }

        $membership = $this->memberships->decode($entry->payload);

        if (!$membership->valid) {
            return $miss('corrupt_payload');
        }

        $rowPrefix = $this->keys->rowPrefix($plan->root, $generation);
        $unique = [];

        // Memberships may repeat a token, so index the reply by key, not position.
        foreach ($membership->ids as $token) {
            $unique[$rowPrefix . $token] = true;
        }

        $fetched = $this->store->mget([
            ...array_keys($unique),
            ...$this->states->pendingStateKeys($plan, $namespace),
        ]);
        $state = $this->states->resolve(
            $plan,
            $namespace,
            $queryHash,
            prefetched: $fetched,
        );

        if (
            $state->version !== $version
            || $state->generation !== $generation
            || $membership->epoch !== $state->epoch
            || $membership->generation !== $state->generation
            || $membership->versions !== $state->versions
            || $membership->tagVersion !== $state->tag
        ) {
            return new CacheRead($state, ReadOutcome::MISS);
        }

        return $this->assembleCanonicalRead($query, $plan, $state, $membership->ids, $fetched);
    }

    public function loadCanonical(QueryBuilder $query, QueryPlan $plan, CacheState $state): ?LoadedCanonical
    {
        $known = $this->container->make(QueryPlanner::class)->knownPrimaryKeys($query, $plan);
        $tokens = $known['tokens'] ?? [];
        $allowMissingCandidates = $known !== null;
        $rowKeys = [];

        if ($known === null) {
            $idsQuery = $query->clone()->select($plan->primaryKey->column);
            $ids = $query->getConnection()->select($idsQuery->toSql(), $idsQuery->getBindings(), false);
            foreach ($ids as $id) {
                $token = $plan->primaryKey->token($id->{$plan->primaryKey->column});
                if ($token === null) {
                    return null;
                }
                $tokens[] = $token;
            }
        }
        foreach ($tokens as $token) {
            $rowKeys[$token] = $this->keys->row($plan->root, $state->generation, $token);
        }

        $atomicMget = $this->store->supportsAtomicMget();
        $fetched = $this->store->mget([
            ...array_values($rowKeys),
            ...($atomicMget ? $this->states->validationKeys($plan, $state) : []),
        ]);

        if ($atomicMget && !$this->states->isCurrentFrom($plan, $state, $fetched)) {
            return null;
        }

        $result = $this->assembleCanonicalBuild(
            $query,
            $plan,
            $state,
            $tokens,
            $fetched,
            requiredColumn: $known === null ? null : $query->deletedAtColumn(),
            visibilityMode: $known['softDeleteMode'] ?? null,
            allowMissingCandidates: $allowMissingCandidates,
        );

        if ($result === null) {
            return null;
        }

        $allReused = count($result->reusedTokens) === count($tokens);

        if ($atomicMget && $allReused) {
            return $result;
        }

        return $this->states->isCurrent($plan, $state)
            ? $result
            : null;
    }

    /**
     * @param  list<string>  $tokens
     * @param  array<string, ?string>  $fetched
     * @return array{0: array<int, \stdClass>, 1: array<int, string>, 2: bool}
     */
    private function decodeCachedRows(
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        array $fetched,
        ?string $requiredColumn = null,
    ): array {
        $rowPrefix = $this->keys->rowPrefix($plan->root, $state->generation);
        $rows = [];
        $missing = [];
        $corrupt = false;

        foreach ($tokens as $index => $token) {
            $rawRow = $fetched[$rowPrefix . $token] ?? null;

            if ($rawRow === null) {
                $missing[$index] = $token;

                continue;
            }

            $row = $this->codec->decodeRowObject($rawRow, $state->epoch, $plan->primaryKey, $token);

            if ($row === null || ($requiredColumn !== null && !property_exists($row, $requiredColumn))) {
                $missing[$index] = $token;
                $corrupt = true;

                continue;
            }

            $rows[$index] = $row;
        }

        return [$rows, $missing, $corrupt];
    }

    /**
     * @param  list<string>  $tokens
     * @param  array<string, ?string>  $fetched
     */
    private function assembleCanonicalRead(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        array $fetched,
    ): CacheRead {
        [$rows, $missing, $corrupt] = $this->decodeCachedRows($plan, $state, $tokens, $fetched);

        if ($missing === []) {
            return new CacheRead($state, ReadOutcome::HIT, array_values($rows));
        }

        $repairer = $this->container->make(RowRepairer::class);
        $repair = $repairer->repair($query, $plan, $state, array_values($missing));

        if ($repair === null) {
            return new CacheRead($state, ReadOutcome::MISS, [], $corrupt ? 'corrupt_payload' : 'row_repair_failed');
        }

        foreach ($missing as $index => $token) {
            if (!isset($repair->rows[$token])) {
                return new CacheRead($state, ReadOutcome::MISS, [], 'row_repair_failed');
            }

            $rows[$index] = $repair->rows[$token];
        }

        ksort($rows);

        return new CacheRead(
            $state,
            $repair->outcome,
            array_values($rows),
            $corrupt ? 'corrupt_payload' : ($repair->outcome === ReadOutcome::REPAIRED ? 'row_repair' : null),
        );
    }

    /**
     * @param  list<string>  $tokens
     * @param  array<string, ?string>  $fetched
     */
    private function assembleCanonicalBuild(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        array $fetched,
        ?string $requiredColumn = null,
        ?string $visibilityMode = null,
        bool $allowMissingCandidates = false,
    ): ?LoadedCanonical {
        [$rows, $missing] = $this->decodeCachedRows(
            $plan,
            $state,
            $tokens,
            $fetched,
            $requiredColumn,
        );
        $reused = [];
        foreach (array_keys($rows) as $index) {
            $reused[$tokens[$index]] = true;
        }

        if ($missing !== []) {
            $repairer = $this->container->make(RowRepairer::class);
            $loaded = $repairer->loadForCanonicalBuild($query, $plan, array_values($missing), $allowMissingCandidates);

            if ($loaded === null) {
                return null;
            }

            foreach ($missing as $index => $token) {
                if (isset($loaded[$token])) {
                    $rows[$index] = $loaded[$token];

                    continue;
                }

                if (!$allowMissingCandidates) {
                    return null;
                }
            }

            ksort($rows);
        }

        if ($requiredColumn !== null) {
            foreach ($rows as $row) {
                if (!property_exists($row, $requiredColumn)) {
                    return null;
                }
            }

            $rows = array_filter($rows, static function (\stdClass $row) use ($requiredColumn, $visibilityMode): bool {
                $deleted = $row->{$requiredColumn} !== null;

                return $visibilityMode === 'default' ? !$deleted : ($visibilityMode === 'only' ? $deleted : true);
            });
        }

        return new LoadedCanonical(array_values($rows), $reused);
    }

    private function queryBuildLease(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        TableQueryReply $entry,
    ): ?BuildLease {
        if ($entry->kind !== RedisProtocol::MISS || $entry->ownerToken === null) {
            return null;
        }

        return new BuildLease(
            $entry->isOwner,
            $this->keys->queryBuild($plan->root, $entry->version, $namespace, $queryHash),
            $this->keys->wake($plan->root, 'q', $queryHash, $entry->ownerToken),
            $entry->ownerToken,
        );
    }

    /**
     * @param  array<int, mixed>  $rows
     */
    public function publishCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        int $wakeTtl,
        ?string $resultOverlay = null,
        array $reusedTokens = [],
    ): bool {
        $ids = [];
        $rowKeys = [];
        $rowPayloads = [];
        $seenRows = [];
        $rowPrefix = $this->keys->rowPrefix($plan->root, $state->generation);

        foreach ($rows as $row) {
            if (!$row instanceof \stdClass || !property_exists($row, $plan->primaryKey->column)) {
                $this->container->make(BuildLeaseCoordinator::class)->release($lease);

                return false;
            }

            $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});

            if ($token === null) {
                $this->container->make(BuildLeaseCoordinator::class)->release($lease);

                return false;
            }

            $ids[] = $token;

            if (isset($seenRows[$token])) {
                if ((array) $seenRows[$token] !== (array) $row) {
                    $this->container->make(BuildLeaseCoordinator::class)->release($lease);

                    return false;
                }

                continue;
            }

            $seenRows[$token] = $row;
            if (!isset($reusedTokens[$token])) {
                $encoded = $this->codec->encodeRow($row, $state->epoch);
                if ($encoded === null) {
                    $this->container->make(BuildLeaseCoordinator::class)->release($lease);

                    return false;
                }
                $rowKeys[] = $rowPrefix . $token;
                $rowPayloads[] = $encoded;
            }
        }

        $membership = $this->memberships->encode(
            epoch: $state->epoch,
            generation: $state->generation,
            ids: $ids,
            versions: $state->versions,
            tagVersion: $state->tag,
        );

        return $this->store->publishCanonical(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            membershipKey: $state->key,
            rowKeys: $rowKeys,
            rowPayloads: $rowPayloads,
            expectedVersion: $state->version,
            expectedGeneration: $state->generation,
            membershipPayload: $membership,
            membershipTtl: $query->configuredTtl() ?? $this->config->queryTtl,
            rowTtl: $this->config->rowTtl,
            buildingKey: $lease->buildingKey,
            wakeKey: (string) $lease->wakeKey,
            token: (string) $lease->token,
            wakeTtl: $wakeTtl,
            resultPayload: $resultOverlay,
        );
    }

    public function readResult(CacheState $state, mixed $raw): CacheRead
    {
        if (!is_string($raw)) {
            return new CacheRead($state, ReadOutcome::MISS);
        }

        $payload = $this->codec->decode($raw);

        if (!$payload->valid) {
            return new CacheRead($state, ReadOutcome::MISS, [], 'corrupt_payload');
        }

        return $payload->epoch === $state->epoch
            && $payload->versions === $state->versions
            && $payload->tagVersion === $state->tag
                ? new CacheRead($state, ReadOutcome::HIT, $payload->rows)
                : new CacheRead($state, ReadOutcome::MISS);
    }

    /** @param array<int, mixed> $rows */
    public function publishResult(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        int $wakeTtl,
    ): bool {
        $encoded = $this->codec->encode(
            $rows,
            $state->epoch,
            $state->versions,
            $state->tag,
        );

        if ($encoded === null) {
            $this->container->make(BuildLeaseCoordinator::class)->release($lease);

            return false;
        }

        $versionKeys = $plan->isResult()
            ? [$this->keys->version($plan->root)]
            : [];
        $expected = $plan->isResult()
            ? [$state->version]
            : [];

        return $this->store->publishVersionedEntries(
            entryKeys: [$state->key],
            entryPayloads: [$encoded],
            ttl: $query->configuredTtl() ?? $this->config->queryTtl,
            versionKeys: $versionKeys,
            expectedVersions: $expected,
            buildingKey: $lease->buildingKey,
            wakeKey: $lease->wakeKey,
            token: $lease->token,
            wakeTtl: $wakeTtl,
            entryFields: ['r'],
        );
    }

    /** @param array<int, mixed> $rows */
    public function inlineResult(CacheState $state, array $rows): ?string
    {
        if (
            $this->config->maxAutoOverlayRows === 0
            || count($rows) > $this->config->maxAutoOverlayRows + self::PAGINATION_LOOKAHEAD_ROWS
        ) {
            return null;
        }

        $encoded = $this->encodeOverlay($state, $rows);

        if ($encoded === null) {
            return null;
        }

        return $state->versions === []
            && $state->tagKey === null
            && strlen($encoded) > self::EAGER_AUTO_OVERLAY_BYTES
                ? null
                : $encoded;
    }

    /** @param array<int, mixed> $rows */
    private function encodeOverlay(CacheState $state, array $rows): ?string
    {
        $encoded = $this->codec->encode(
            $rows,
            $state->epoch,
            $state->versions,
            $state->tag,
        );

        return $encoded === null || strlen($encoded) > self::MAX_AUTO_OVERLAY_BYTES ? null : $encoded;
    }
}
