<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisProtocol;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\OverlayAdmission;
use NormCache\Values\QueryPlan;
use NormCache\Values\RowRepair;

final readonly class QueryEntryRepository
{
    private const MAX_AUTO_OVERLAY_BYTES = 128 * 1024;

    private const PAGINATION_LOOKAHEAD_ROWS = 1;

    private const ESTIMATE_PROBE_MIN_ROWS = 32;

    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private CacheStateResolver $states,
        private RawResultCodec $codec,
        private MembershipCodec $memberships,
        private BuildLeaseCoordinator $leases,
    ) {}

    /**
     * @param  \Closure(CacheState, list<string>): ?RowRepair  $repair
     */
    public function readCanonical(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $head,
        bool $repairMissing,
        \Closure $repair,
        bool $rootVersionApproved = false,
    ): CacheRead {
        $status = RedisProtocol::status($head);
        $version = RedisProtocol::version($head);
        $generation = RedisProtocol::version($head, 2);
        $rawMembership = $status === RedisProtocol::HIT
            ? RedisProtocol::canonicalPayload($head)
            : null;
        $miss = fn(?string $reason = null): CacheRead => new CacheRead(
            $this->states->resolve($plan, $namespace, $queryHash, $version, $generation),
            ReadOutcome::MISS,
            [],
            $reason,
        );

        if (!is_string($rawMembership)) {
            return $miss();
        }

        $membership = $this->memberships->decode($rawMembership);

        if (!$membership->valid) {
            return $miss('corrupt_payload');
        }

        $rowPrefix = $this->keys->rowPrefix($plan->root, $generation);
        $unique = [];

        // Membership tokens are not unique.
        foreach ($membership->ids as $token) {
            $unique[$rowPrefix . $token] = true;
        }

        [$state, $fetched] = $this->states->resolveCanonical(
            $plan,
            $namespace,
            $queryHash,
            array_keys($unique),
        );

        if (
            $state->version !== $version
            || $state->generation !== $generation
            || $membership->epoch !== $state->epoch
            || $membership->generation !== $state->generation
            || $membership->versions !== $state->versions
            || $membership->tagVersion !== $state->tag
            || (!$rootVersionApproved && $membership->rootVersion !== $state->version)
        ) {
            return new CacheRead(
                $state,
                ReadOutcome::MISS,
                staleMembership: $membership,
                staleMembershipRaw: $rawMembership,
            );
        }

        if ($membership->ids === []) {
            return new CacheRead(
                $state,
                ReadOutcome::HIT,
                overlayRejected: $membership->overlayRejected,
            );
        }

        $rows = [];
        $missingAt = [];
        $corrupt = false;

        foreach ($membership->ids as $index => $token) {
            $rowKey = $rowPrefix . $token;
            $rawRow = $fetched[$rowKey] ?? null;

            if ($rawRow === null) {
                $missingAt[$index] = $token;

                continue;
            }

            $row = $this->codec->decodeRowObject($rawRow, $state->epoch, $plan->primaryKey, $token);

            if ($row === null) {
                $corrupt = true;
                $missingAt[$index] = $token;

                continue;
            }

            $rows[$index] = $row;
        }

        $outcome = ReadOutcome::HIT;

        if ($missingAt !== []) {
            if (!$repairMissing) {
                return new CacheRead($state, ReadOutcome::MISS);
            }

            $repairResult = $repair($state, array_values($missingAt));

            if ($repairResult === null) {
                return new CacheRead($state, ReadOutcome::MISS);
            }

            $repaired = $repairResult->rows;
            $outcome = $repairResult->outcome;

            foreach ($missingAt as $index => $token) {
                if (!isset($repaired[$token])) {
                    return new CacheRead($state, ReadOutcome::MISS);
                }

                $rows[$index] = $repaired[$token];
            }

            ksort($rows);
            $rows = array_values($rows);
        }

        return new CacheRead(
            $state,
            $outcome,
            $rows,
            $corrupt
                ? 'corrupt_payload'
                : ($outcome === ReadOutcome::REPAIRED ? 'row_repair' : null),
            $membership->overlayRejected,
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
        bool $overlayRejected = false,
    ): bool {
        $ids = [];
        $rowKeys = [];
        $rowPayloads = [];
        $positions = [];
        $rowPrefix = $this->keys->rowPrefix($plan->root, $state->generation);

        foreach ($rows as $row) {
            if (!$row instanceof \stdClass || !property_exists($row, $plan->primaryKey->column)) {
                return false;
            }

            $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});

            if ($token === null) {
                return false;
            }

            $encoded = $this->codec->encodeRow($row, $state->epoch);
            $ids[] = $token;

            if (array_key_exists($token, $positions)) {
                if ($rowPayloads[$positions[$token]] !== $encoded) {
                    return false;
                }

                continue;
            }

            $positions[$token] = count($rowKeys);
            $rowKeys[] = $rowPrefix . $token;
            $rowPayloads[] = $encoded;
        }

        $membership = $this->memberships->encode(
            epoch: $state->epoch,
            generation: $state->generation,
            ids: $ids,
            versions: $state->versions,
            tagVersion: $state->tag,
            overlayRejected: $overlayRejected,
            rootVersion: $state->version,
        );

        // Rows publish in guarded slices; membership follows once they are durable.
        if (!$this->store->publishRows(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            buildingKey: $lease->buildingKey,
            rowKeys: $rowKeys,
            rowPayloads: $rowPayloads,
            expectedVersion: $state->version,
            expectedGeneration: $state->generation,
            rowTtl: $this->config->rowTtl,
            token: (string) $lease->token,
            leaseTtl: $this->config->buildingLockTtl,
        )) {
            return false;
        }

        return $this->store->publishCanonical(
            versionKey: $this->keys->version($plan->root),
            generationKey: $this->keys->generation($plan->root),
            membershipKey: $state->key,
            rowKeys: [],
            rowPayloads: [],
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

    /** @param array<int, mixed> $rows */
    public function restampCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        OverlayAdmission $overlay,
    ): void {
        $ids = [];

        foreach ($rows as $row) {
            if (!$row instanceof \stdClass || !property_exists($row, $plan->primaryKey->column)) {
                return;
            }

            $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});

            if ($token === null) {
                return;
            }

            $ids[] = $token;
        }

        $membership = $this->memberships->encode(
            epoch: $state->epoch,
            generation: $state->generation,
            ids: $ids,
            versions: $state->versions,
            tagVersion: $state->tag,
            overlayRejected: $overlay->rejected,
            rootVersion: $state->version,
        );
        $ttl = $query->configuredTtl() ?? $this->config->queryTtl;

        if ($overlay->payload !== null) {
            $this->store->publishVersionedEntries(
                entryKeys: [$state->key, $state->key],
                entryPayloads: [$membership, $overlay->payload],
                ttl: $ttl,
                versionKeys: [$this->keys->version($plan->root)],
                expectedVersions: [$state->version],
                entryFields: ['m', 'r'],
            );

            return;
        }

        // Drop only after the guarded write accepts this publisher.
        if ($this->store->publishVersionedEntries(
            entryKeys: [$state->key],
            entryPayloads: [$membership],
            ttl: $ttl,
            versionKeys: [$this->keys->version($plan->root)],
            expectedVersions: [$state->version],
            entryFields: ['m'],
        )) {
            $this->store->deleteHashField($state->key, 'r');
        }
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
            && $payload->rootVersion === $state->version
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
            $state->version,
            $state->versions,
            $state->tag,
        );

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
    public function promoteResult(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $sourceState,
        string $namespace,
        string $queryHash,
        array $rows,
    ): bool {
        $lease = null;

        try {
            $encoded = $this->encodeResultWithinLimits($rows, $sourceState);

            if ($encoded === null) {
                return false;
            }

            $ttl = $query->configuredTtl() ?? $this->config->queryTtl;
            $resultState = new CacheState(
                key: $this->keys->queryEntry(
                    $plan->root,
                    $namespace,
                    $queryHash,
                ),
                epoch: $sourceState->epoch,
                version: $sourceState->version,
                generation: $plan->usesGeneration() ? $sourceState->generation : '0',
                versions: $sourceState->versions,
                tag: $sourceState->tag,
                tagKey: $sourceState->tagKey,
            );
            $lease = $this->leases->claim($plan, $resultState, $namespace, $queryHash);

            if (!$lease->owner) {
                return false;
            }

            $current = $this->states->resolve($plan, $namespace, $queryHash);

            if (!$current->equals($resultState)) {
                $this->leases->release($lease);

                return false;
            }

            return $this->store->publishVersionedEntries(
                entryKeys: [$resultState->key],
                entryPayloads: [$encoded],
                ttl: $ttl,
                versionKeys: [$this->keys->version($plan->root)],
                expectedVersions: [$resultState->version],
                buildingKey: $lease->buildingKey,
                wakeKey: $lease->wakeKey,
                token: $lease->token,
                wakeTtl: $this->config->wakeTtl(),
                entryFields: ['r'],
            );
        } catch (\Throwable $exception) {
            if ($lease !== null && $lease->owner) {
                $this->leases->release($lease);
            }

            $this->runtime->fail($exception);

            return false;
        }
    }

    /**
     * @param  array<int, mixed>  $rows
     */
    public function inlineResult(CacheState $state, array $rows): OverlayAdmission
    {
        if ($this->config->maxAutoOverlayRows === 0) {
            return OverlayAdmission::notAttempted();
        }

        try {
            $encoded = $this->encodeResultWithinLimits($rows, $state);
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return OverlayAdmission::notAttempted();
        }

        return $encoded === null
            ? OverlayAdmission::rejected()
            : OverlayAdmission::accepted($encoded);
    }

    public function rebuiltResultOutcome(CacheRead $read, bool $promoted): CacheRead
    {
        return $promoted
            ? $read->asRepaired('result_overlay_rebuilt')
            : $read->withReason('corrupt_result_overlay_fallback');
    }

    /** @param array<int, mixed> $rows */
    private function encodeResultWithinLimits(array $rows, CacheState $state): ?string
    {
        $count = count($rows);

        if (
            $this->config->maxAutoOverlayRows === 0
            || $count > $this->config->maxAutoOverlayRows + self::PAGINATION_LOOKAHEAD_ROWS
        ) {
            return null;
        }

        if (
            $count > self::ESTIMATE_PROBE_MIN_ROWS
            && $this->resultExceedsEstimate($rows, $state, $count)
        ) {
            return null;
        }

        $encoded = $this->codec->encode(
            $rows,
            $state->epoch,
            $state->version,
            $state->versions,
            $state->tag,
        );

        return strlen($encoded) > self::MAX_AUTO_OVERLAY_BYTES ? null : $encoded;
    }

    /** @param array<int, mixed> $rows */
    private function resultExceedsEstimate(array $rows, CacheState $state, int $count): bool
    {
        $first = $this->encodedResultLength($rows, $state, 1);
        $marginal = $count < 2 ? 0 : $this->encodedResultLength($rows, $state, 2) - $first;

        return $first + $marginal * ($count - 1) > self::MAX_AUTO_OVERLAY_BYTES;
    }

    /** @param array<int, mixed> $rows */
    private function encodedResultLength(array $rows, CacheState $state, int $take): int
    {
        return strlen($this->codec->encode(
            array_slice($rows, 0, $take),
            $state->epoch,
            $state->version,
            $state->versions,
            $state->tag,
        ));
    }
}
