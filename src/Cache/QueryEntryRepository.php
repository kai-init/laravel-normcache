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
use NormCache\Values\RowRepair;

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
            $arguments = [
                $this->keys->version($plan->root),
                $this->keys->generation($plan->root),
                $this->keys->tablePrefix($plan->root),
                $namespace,
                $queryHash,
            ];
            $claimToken = $claim ? bin2hex(random_bytes(16)) : null;
            if ($claimToken !== null) {
                $arguments[] = $claimToken;
                $arguments[] = $this->config->buildingLockTtl;
            }
            $head = $this->config->maxAutoOverlayRows > 0
                ? $this->store->fetchResultOrCanonical(...$arguments)
                : $this->store->fetchCanonical(...$arguments);

            if (RedisProtocol::status($head) === RedisProtocol::RESULT) {
                $state = $this->states->resolve(
                    $plan,
                    $namespace,
                    $queryHash,
                    RedisProtocol::version($head),
                    usesGeneration: false,
                );
                $result = $this->readResult($state, RedisProtocol::resultPayload($head));

                return $result->served()
                    ? $result->withReason('result_overlay')
                    : new CacheRead(
                        $this->states->resolve($plan, $namespace, $queryHash),
                        ReadOutcome::MISS,
                        [],
                        $result->reason,
                    );
            }

            if (RedisProtocol::status($head) === RedisProtocol::MEMBERSHIP) {
                $head[0] = RedisProtocol::HIT;
            }

            return $this->readCanonical($query, $plan, $namespace, $queryHash, $head);
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
            $entry = $this->store->fetchResult(
                $this->keys->version($plan->root),
                $this->keys->tablePrefix($plan->root),
                $namespace,
                $queryHash,
                $claimToken,
                $claimToken === null ? 0 : $this->config->buildingLockTtl,
            );
            $raw = RedisProtocol::value($entry, 1);
            $version = RedisProtocol::version($entry, 0);
            $lease = !is_string($raw)
                && is_string($entry[3] ?? null)
                && $entry[3] !== ''
                    ? new BuildLease(
                        (int) ($entry[2] ?? 0) === 1,
                        $this->keys->queryBuild($plan->root, $version, $namespace, $queryHash),
                        $this->keys->wake($plan->root, 'q', $queryHash, $entry[3]),
                        $entry[3],
                    )
                    : null;
            try {
                $state = $this->states->resolve(
                    $plan,
                    $namespace,
                    $queryHash,
                    $version,
                    usesGeneration: false,
                );
                $result = $this->readResult($state, $raw);
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
        array $head,
    ): CacheRead {
        $status = RedisProtocol::status($head);
        $version = RedisProtocol::version($head);
        $generation = RedisProtocol::version($head, 2);
        $rawMembership = $status === RedisProtocol::HIT
            ? RedisProtocol::canonicalPayload($head)
            : null;
        $lease = $status === RedisProtocol::MISS
            && is_string($head[4] ?? null)
            && $head[4] !== ''
                ? new BuildLease(
                    (int) ($head[3] ?? 0) === 1,
                    $this->keys->queryBuild($plan->root, $version, $namespace, $queryHash),
                    $this->keys->wake($plan->root, 'q', $queryHash, $head[4]),
                    $head[4],
                )
                : null;
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

        if (!is_string($rawMembership)) {
            return $miss(null);
        }

        $membership = $this->memberships->decode($rawMembership);

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

        $result = $this->assembleRows($query, $plan, $state, $membership->ids, $fetched);

        return $result;
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

        $reusedTokens = [];
        $result = $this->assembleRows(
            $query,
            $plan,
            $state,
            $tokens,
            $fetched,
            coldBuild: true,
            reusedTokens: $reusedTokens,
            requiredColumn: $known === null ? null : $query->deletedAtColumn(),
            visibilityMode: $known['softDeleteMode'] ?? null,
            allowMissingCandidates: $allowMissingCandidates,
        );

        if (!$result->served()) {
            return null;
        }

        if ($atomicMget && $result->outcome === ReadOutcome::HIT) {
            return new LoadedCanonical($result->rows, $reusedTokens);
        }

        return $this->states->isCurrent($plan, $state)
            ? new LoadedCanonical($result->rows, $reusedTokens)
            : null;
    }

    /**
     * @param  list<string>  $tokens
     * @param  array<string, ?string>  $fetched
     */
    private function assembleRows(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        array $fetched,
        bool $coldBuild = false,
        ?array &$reusedTokens = null,
        ?string $requiredColumn = null,
        ?string $visibilityMode = null,
        bool $allowMissingCandidates = false,
    ): CacheRead {
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
            if ($reusedTokens !== null) {
                $reusedTokens[$token] = true;
            }
        }

        $outcome = ReadOutcome::HIT;

        if ($missing !== []) {
            $repairer = $this->container->make(RowRepairer::class);
            $repair = $coldBuild
                ? $repairer->loadForCanonicalBuild($query, $plan, array_values($missing), $allowMissingCandidates)
                : $repairer->repair($query, $plan, $state, array_values($missing));

            if ($repair === null) {
                return new CacheRead($state, ReadOutcome::MISS, [], $corrupt ? 'corrupt_payload' : 'row_repair_failed');
            }

            $repairRows = $repair instanceof RowRepair ? $repair->rows : $repair;

            foreach ($missing as $index => $token) {
                if (isset($repairRows[$token])) {
                    $rows[$index] = $repairRows[$token];

                    continue;
                }

                if (!$allowMissingCandidates) {
                    return new CacheRead($state, ReadOutcome::MISS, [], 'row_repair_failed');
                }
            }

            ksort($rows);
            $outcome = $repair instanceof RowRepair ? $repair->outcome : ReadOutcome::REPAIRED;
        }

        if ($requiredColumn !== null) {
            foreach ($rows as $row) {
                if (!property_exists($row, $requiredColumn)) {
                    return new CacheRead($state, ReadOutcome::MISS, [], 'row_repair_failed');
                }
            }

            $rows = array_filter($rows, static function (\stdClass $row) use ($requiredColumn, $visibilityMode): bool {
                $deleted = $row->{$requiredColumn} !== null;

                return $visibilityMode === 'default' ? !$deleted : ($visibilityMode === 'only' ? $deleted : true);
            });
        }

        return new CacheRead(
            $state,
            $outcome,
            array_values($rows),
            $corrupt ? 'corrupt_payload' : ($outcome === ReadOutcome::REPAIRED ? 'row_repair' : null),
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
                return false;
            }

            $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});

            if ($token === null) {
                return false;
            }

            $ids[] = $token;

            if (isset($seenRows[$token])) {
                if ((array) $seenRows[$token] !== (array) $row) {
                    return false;
                }

                continue;
            }

            $seenRows[$token] = $row;
            if (!isset($reusedTokens[$token])) {
                $encoded = $this->codec->encodeRow($row, $state->epoch);
                if ($encoded === null) {
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
