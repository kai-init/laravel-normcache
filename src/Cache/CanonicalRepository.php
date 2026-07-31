<?php

namespace NormCache\Cache;

use Closure;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\RowRepair;

final readonly class CanonicalRepository
{
    public function __construct(
        private CacheConfig $config,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private CacheStateResolver $states,
        private RawResultCodec $codec,
        private MembershipCodec $memberships,
    ) {}

    /**
     * @param  Closure(CacheState, list<string>): ?RowRepair  $repair
     */
    public function read(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $head,
        bool $repairMissing,
        Closure $repair,
    ): CacheRead {
        $status = $head[0] ?? null;
        $version = is_string($head[1] ?? null) ? $head[1] : '0';
        $generation = is_string($head[2] ?? null) ? $head[2] : '0';
        $rawMembership = $status === 'hit' ? ($head[3] ?? null) : null;
        $miss = fn(?string $reason): CacheRead => new CacheRead(
            $this->states->resolve($plan, $namespace, $queryHash, $version, $generation)[0],
            ReadOutcome::MISS,
            [],
            $reason,
        );

        if (!is_string($rawMembership)) {
            return $miss($status === 'corrupt' ? 'corrupt_payload' : null);
        }

        $membership = $this->memberships->decode($rawMembership);

        if (!$membership->valid) {
            return $miss('corrupt_payload');
        }

        $rowPrefix = $this->keys->tablePrefix($plan->root) . ':r:g' . $generation . ':';
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

        if ($membership->ids === []) {
            return new CacheRead($state, ReadOutcome::HIT);
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
        );
    }

    /**
     * @param  array<int, mixed>  $rows
     * @param  array{0: string, 1: string}|null  $resultOverlay  [key, payload] published
     *                                                           under this operation's guard
     */
    public function publish(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        int $wakeTtl,
        ?array $resultOverlay = null,
    ): bool {
        $ids = [];
        $encodedRows = [];

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
            $encodedRows[$this->keys->row($plan->root, $state->generation, $token)] = $encoded;
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
            rows: $encodedRows,
            expectedVersion: $state->version,
            expectedGeneration: $state->generation,
            membershipPayload: $membership,
            membershipTtl: $query->configuredTtl() ?? $this->config->queryTtl,
            rowTtl: $this->config->rowTtl,
            buildingKey: $lease->buildingKey,
            wakeKey: (string) $lease->wakeKey,
            token: (string) $lease->token,
            wakeTtl: $wakeTtl,
            resultKey: $resultOverlay[0] ?? null,
            resultPayload: $resultOverlay[1] ?? null,
        );
    }
}
