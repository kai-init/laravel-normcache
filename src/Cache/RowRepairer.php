<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\RowRepair;

final readonly class RowRepairer
{
    /** Keeps WHERE IN bindings below SQLite's traditional variable limit. */
    private const REPAIR_BATCH_SIZE = 900;

    public function __construct(
        private CacheConfig $config,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private QueryIdentity $identity,
        private RawResultCodec $codec,
        private CacheStateResolver $states,
        private BuildLeaseCoordinator $leases,
        private FailureReporter $failures,
    ) {}

    /** @param list<string> $tokens */
    public function repair(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
    ): ?RowRepair {
        $tokens = array_values(array_unique($tokens));
        sort($tokens, SORT_STRING);

        $repairHash = $this->identity->repairHash(
            $plan->root->hash,
            $state->generation,
            $tokens,
        );
        $lease = $this->leases->claimRepair($plan->root, $repairHash);

        if (!$lease->owner) {
            if ($lease->wakeKey !== null) {
                $this->store->brpop(
                    $lease->wakeKey,
                    $this->config->stampedeWaitMs / 1000,
                );
            }

            $rows = $this->readRepaired($plan, $state, $tokens);

            if ($rows === null || !$this->states->isCurrent($plan, $state)) {
                return null;
            }

            return new RowRepair($rows, ReadOutcome::HIT);
        }

        try {
            $rows = $this->build($query, $plan, $state, $tokens, $lease);
        } catch (\Throwable $exception) {
            $this->leases->release($lease);

            throw $exception;
        }

        // Null means no publication script ran, so this caller still owns the lease.
        if ($rows === null) {
            $this->leases->release($lease);

            return null;
        }

        return new RowRepair($rows, ReadOutcome::REPAIRED);
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, \stdClass>|null
     */
    private function build(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        BuildLease $lease,
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

        $rowsByToken = [];

        try {
            foreach (array_chunk($values, self::REPAIR_BATCH_SIZE) as $batch) {
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
        } catch (\Throwable $exception) {
            // Records without disabling: a database fault is not a cache fault.
            $this->failures->repairUnreachable($exception, $plan->root, count($tokens));

            return null;
        }

        if (!$this->states->isCurrent($plan, $state)) {
            return null;
        }

        $rowPrefix = $this->keys->rowPrefix($plan->root, $state->generation);
        $rowKeys = [];
        $rowPayloads = [];

        foreach ($tokens as $token) {
            if (!isset($rowsByToken[$token])) {
                return null;
            }

            $rowKeys[] = $rowPrefix . $token;
            $rowPayloads[] = $this->codec->encodeRow($rowsByToken[$token], $state->epoch);
        }

        if (!$this->store->publishVersionedEntries(
            entryKeys: $rowKeys,
            entryPayloads: $rowPayloads,
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
            wakeKey: (string) $lease->wakeKey,
            token: (string) $lease->token,
            wakeTtl: $this->config->wakeTtl(),
        )) {
            return [];
        }

        return $this->states->isCurrent($plan, $state) ? $rowsByToken : [];
    }

    /**
     * @param  list<string>  $tokens
     * @return array<string, \stdClass>|null
     */
    private function readRepaired(
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
}
