<?php

namespace NormCache\Cache;

use Illuminate\Contracts\Container\Container;
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
use NormCache\Values\QueryPlan;

final readonly class QueryEntryRepository
{
    private const MAX_AUTO_OVERLAY_BYTES = 128 * 1024;

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
        $miss = fn(?string $reason): CacheRead => new CacheRead(
            $this->states->resolve($plan, $namespace, $queryHash, $version, $generation),
            ReadOutcome::MISS,
            [],
            $reason,
        );

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
        ) {
            return new CacheRead($state, ReadOutcome::MISS);
        }

        return $this->assembleRows($query, $plan, $state, $membership->ids, $fetched);
    }

    /** @return list<\stdClass>|null */
    public function loadCanonical(QueryBuilder $query, QueryPlan $plan, CacheState $state): ?array
    {
        $idsQuery = $query->clone()->select($plan->primaryKey->column);
        $ids = $query->getConnection()->select($idsQuery->toSql(), $idsQuery->getBindings(), false);
        $tokens = [];
        $rowKeys = [];

        foreach ($ids as $id) {
            $token = $plan->primaryKey->token($id->{$plan->primaryKey->column});

            if ($token === null) {
                return null;
            }

            $tokens[] = $token;
            $rowKeys[$token] = $this->keys->row($plan->root, $state->generation, $token);
        }

        $result = $this->assembleRows(
            $query,
            $plan,
            $state,
            $tokens,
            $this->store->mget(array_values($rowKeys)),
        );

        return $result->served() && $this->states->isCurrent($plan, $state)
            ? $result->rows
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

            if ($row === null) {
                $missing[$index] = $token;
                $corrupt = true;

                continue;
            }

            $rows[$index] = $row;
        }

        $outcome = ReadOutcome::HIT;

        if ($missing !== []) {
            $repair = $this->container->make(RowRepairer::class)->repair($query, $plan, $state, array_values($missing));

            if ($repair === null) {
                return new CacheRead($state, ReadOutcome::MISS, [], $corrupt ? 'corrupt_payload' : 'row_repair_failed');
            }

            foreach ($missing as $index => $token) {
                $rows[$index] = $repair->rows[$token];
            }

            ksort($rows);
            $outcome = $repair->outcome;
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

            if ($encoded === null) {
                return false;
            }

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
        $count = count($rows);

        if (
            $this->config->maxAutoOverlayRows === 0
            || $count > $this->config->maxAutoOverlayRows + self::PAGINATION_LOOKAHEAD_ROWS
        ) {
            return null;
        }

        $encoded = $this->codec->encode(
            $rows,
            $state->epoch,
            $state->versions,
            $state->tag,
        );

        return $encoded === null || strlen($encoded) > self::MAX_AUTO_OVERLAY_BYTES ? null : $encoded;
    }
}
