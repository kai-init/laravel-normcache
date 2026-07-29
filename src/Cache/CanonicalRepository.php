<?php

namespace NormCache\Cache;

use Closure;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\CacheReadOutcome;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;

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
     * @param  Closure(CacheState, list<string>): array{rows: array<string, \stdClass>|null, outcome: CacheReadOutcome}  $repair
     * @return array{0: CacheState, 1: array{hit: bool, rows: array, reason: ?string, outcome?: CacheReadOutcome}}
     */
    public function read(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $head,
        bool $repairMissing,
        Closure $repair,
    ): array {
        $status = $head[0] ?? null;
        $version = is_string($head[1] ?? null) ? $head[1] : '0';
        $generation = is_string($head[2] ?? null) ? $head[2] : '0';
        $rawMembership = $status === 'hit' ? ($head[3] ?? null) : null;
        $miss = fn(?string $reason): array => [
            $this->states->resolve($plan, $namespace, $queryHash, $version, $generation)[0],
            ['hit' => false, 'rows' => [], 'reason' => $reason],
        ];

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

        $stateKeys = $this->states->canonicalKeys($plan, $namespace);
        $fetched = $this->states->fetch(array_keys($unique), $stateKeys['final']);
        $state = $this->states->canonicalFromFetched(
            $plan,
            $namespace,
            $queryHash,
            $stateKeys,
            $fetched,
        );

        if (
            $state->version !== $version
            || $state->generation !== $generation
            || $membership->epoch !== $state->epoch
            || $membership->generation !== $state->generation
            || $membership->versions !== $state->versions
            || $membership->tagVersion !== $state->tag
        ) {
            return [$state, ['hit' => false, 'rows' => [], 'reason' => null]];
        }

        if ($membership->ids === []) {
            return [$state, ['hit' => true, 'rows' => [], 'reason' => null]];
        }

        $rows = [];
        $missingAt = [];
        $corrupt = false;
        $primaryKeyColumn = $plan->primaryKey->column;
        $integerPrimaryKey = $plan->primaryKey->family === 'integer';

        foreach ($membership->ids as $index => $token) {
            $rowKey = $rowPrefix . $token;
            $rawRow = $fetched[$rowKey] ?? null;

            if ($rawRow === null) {
                $missingAt[$index] = $token;

                continue;
            }

            $row = $this->codec->decodeRowObject($rawRow, $state->epoch);
            $primaryKeyValue = $row->{$primaryKeyColumn} ?? null;
            $matchesToken = $integerPrimaryKey
                ? (is_int($primaryKeyValue) || is_string($primaryKeyValue))
                    && substr($token, 2) === (string) $primaryKeyValue
                : is_string($primaryKeyValue)
                    && $token === 's:' . rtrim(strtr(base64_encode($primaryKeyValue), '+/', '-_'), '=');

            if ($row === null || !$matchesToken) {
                $corrupt = true;
                $missingAt[$index] = $token;

                continue;
            }

            $rows[$index] = $row;
        }

        $outcome = CacheReadOutcome::HIT;

        if ($missingAt !== []) {
            if (!$repairMissing) {
                return [$state, ['hit' => false, 'rows' => [], 'reason' => null]];
            }

            $repairResult = $repair($state, array_values($missingAt));
            $repaired = $repairResult['rows'];
            $outcome = $repairResult['outcome'];

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
            'reason' => $corrupt
                ? 'corrupt_payload'
                : ($outcome === CacheReadOutcome::REPAIRED ? 'row_repair' : null),
            'outcome' => $outcome,
        ]];
    }

    /** @param array<int, mixed> $rows */
    public function publish(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        int $wakeTtl,
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
        );
    }
}
