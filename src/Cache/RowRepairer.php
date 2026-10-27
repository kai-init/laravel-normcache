<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\QueryPlanner;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\LoadedCanonical;
use NormCache\Values\QueryPlan;
use NormCache\Values\RowRepair;
use NormCache\Values\TableIdentity;

final readonly class RowRepairer
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private RawResultCodec $codec,
        private CacheStateResolver $states,
        private BuildLeaseCoordinator $leases,
        private FailureReporter $failures,
    ) {}

    /**
     * @param  list<string>  $tokens
     * @param  array<string, ?string>  $fetched
     * @return array{array<int, \stdClass>, array<int, string>, bool}
     */
    public static function decode(
        RawResultCodec $codec,
        QueryPlan $plan,
        CacheState $state,
        array $tokens,
        string $prefix,
        array $fetched,
        ?string $requiredColumn = null,
    ): array {
        $rows = $missing = [];
        $corrupt = false;
        foreach ($tokens as $index => $token) {
            $payload = $fetched[$prefix . $token] ?? null;
            $row = $payload === null ? null : $codec->decodeRowObject($payload, $state->epoch, $plan->primaryKey, $token);
            if ($row === null || ($requiredColumn !== null && !property_exists($row, $requiredColumn))) {
                $missing[$index] = $token;
                $corrupt = $corrupt || $payload !== null;
            } else {
                $rows[$index] = $row;
            }
        }

        return [$rows, $missing, $corrupt];
    }

    public function loadCanonical(QueryBuilder $query, QueryPlan $plan, CacheState $state): ?LoadedCanonical
    {
        $known = app(QueryPlanner::class)->knownPrimaryKeys($query, $plan);
        $tokens = $known['tokens'] ?? [];
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

        $prefix = $this->keys->rowPrefix($plan->root, $state->generation);
        $rowKeys = [];
        foreach ($tokens as $token) {
            $rowKeys[$token] = $prefix . $token;
        }
        try {
            $atomic = $this->store->supportsAtomicMget();
            $fetched = $this->store->mget([
                ...array_values($rowKeys),
                ...($atomic ? $this->states->validationKeys($plan, $state) : []),
            ]);
            if ($atomic && !$this->states->isCurrentFrom($plan, $state, $fetched)) {
                return null;
            }

            $column = $known === null ? null : $query->deletedAtColumn();
            [$rows, $missing] = self::decode(
                $this->codec,
                $plan,
                $state,
                $tokens,
                $prefix,
                $fetched,
                $column,
            );
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return null;
        }
        $reused = [];
        foreach (array_keys($rows) as $index) {
            $reused[$tokens[$index]] = true;
        }
        if ($missing !== []) {
            $loaded = $this->fetchDatabaseRows($query, $plan, array_values(array_unique($missing)), $known !== null);
            if ($loaded === null) {
                return null;
            }
            foreach ($missing as $index => $token) {
                if (isset($loaded[$token])) {
                    $rows[$index] = $loaded[$token];
                }
            }
            ksort($rows);
        }

        if ($column !== null) {
            foreach ($rows as $index => $row) {
                if (!property_exists($row, $column)) {
                    return null;
                }
                $deleted = $row->{$column} !== null;
                if (($known['softDeleteMode'] === 'default' && $deleted) || ($known['softDeleteMode'] === 'only' && !$deleted)) {
                    unset($rows[$index]);
                }
            }
        }

        if ($atomic && $missing === []) {
            return new LoadedCanonical(array_values($rows), $reused);
        }

        try {
            return $this->states->isCurrent($plan, $state)
                ? new LoadedCanonical(array_values($rows), $reused)
                : null;
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return null;
        }
    }

    /** @param array<int, \stdClass> $rows
     * @param  array<int, string>  $missing
     */
    public function assemble(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        array $missing,
        bool $corrupt,
    ): CacheRead {
        $repair = $this->repair($query, $plan, $state, array_values($missing));
        if ($repair === null) {
            return new CacheRead($state, ReadOutcome::MISS, [], $corrupt ? 'corrupt_payload' : 'row_repair_failed');
        }
        foreach ($missing as $index => $token) {
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

    /** @param list<string> $tokens */
    public function repair(QueryBuilder $query, QueryPlan $plan, CacheState $state, array $tokens): ?RowRepair
    {
        $tokens = array_values(array_unique($tokens));
        sort($tokens, SORT_STRING);
        $lease = $this->leases->claimRepair(
            $plan->root,
            $state->generation,
            hash('xxh128', TableIdentity::encodeFields($tokens)),
        );
        $leaseSettled = !$lease->owner;
        try {
            if (!$lease->owner && $lease->wakeKey !== null) {
                $this->store->brpop($lease->wakeKey, $this->config->stampedeWaitMs / 1000);
            }
            // The missing-row decision predates this lease, so owners also recheck.
            [$rows, $missing] = $this->readCached($plan, $state, $tokens);
            if ($missing === []) {
                return $this->states->isCurrent($plan, $state)
                    ? new RowRepair(array_combine($tokens, $rows), ReadOutcome::HIT)
                    : null;
            }
            if (!$lease->owner) {
                return null;
            }

            try {
                $loaded = $this->fetchDatabaseRows($query, $plan, array_values($missing));
            } catch (\Throwable $exception) {
                $this->failures->repairUnreachable($exception, $plan->root, count($missing));

                return null;
            }
            if ($loaded === null || !$this->states->isCurrent($plan, $state)) {
                return null;
            }
            $encoded = $this->encode($plan, $state, array_values($loaded));
            if ($encoded === null) {
                return null;
            }

            $published = $this->store->publishVersionedEntries(
                entryKeys: $encoded['rowKeys'],
                entryPayloads: $encoded['rowPayloads'],
                ttl: $this->config->rowTtl,
                versionKeys: [$this->keys->version($plan->root), $this->keys->generation($plan->root)],
                expectedVersions: [$state->version, $state->generation],
                buildingKey: $lease->buildingKey,
                wakeKey: $lease->wakeKey,
                token: $lease->token,
                wakeTtl: $this->config->wakeTtl(),
            );
            $leaseSettled = true;
            if (!$published || !$this->states->isCurrent($plan, $state)) {
                return null;
            }

            foreach ($missing as $index => $token) {
                $rows[$index] = $loaded[$token];
            }
            ksort($rows);

            return new RowRepair(array_combine($tokens, $rows), ReadOutcome::REPAIRED);
        } finally {
            if (!$leaseSettled) {
                $this->leases->release($lease);
            }
        }
    }

    /**
     * @param  array<int, mixed>  $rows
     * @param  array<string, true>  $reusedTokens
     * @return array{ids: list<string>, rowKeys: list<string>, rowPayloads: list<string>}|null
     */
    public function encode(QueryPlan $plan, CacheState $state, array $rows, array $reusedTokens = []): ?array
    {
        $ids = $keys = $payloads = $seen = [];
        $prefix = $this->keys->rowPrefix($plan->root, $state->generation);
        foreach ($rows as $row) {
            if (!$row instanceof \stdClass || !property_exists($row, $plan->primaryKey->column)) {
                return null;
            }
            $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});
            if ($token === null) {
                return null;
            }
            $ids[] = $token;
            if (isset($seen[$token])) {
                if ((array) $seen[$token] !== (array) $row) {
                    return null;
                }

                continue;
            }
            $seen[$token] = $row;
            if (!isset($reusedTokens[$token])) {
                $payload = $this->codec->encodeRow($row, $state->epoch);
                if ($payload === null) {
                    return null;
                }
                $keys[] = $prefix . $token;
                $payloads[] = $payload;
            }
        }

        return ['ids' => $ids, 'rowKeys' => $keys, 'rowPayloads' => $payloads];
    }

    /** @param list<string> $tokens
     * @return array{array<int, \stdClass>, array<int, string>, bool}
     */
    private function readCached(QueryPlan $plan, CacheState $state, array $tokens): array
    {
        $prefix = $this->keys->rowPrefix($plan->root, $state->generation);
        $keys = array_map(static fn(string $token): string => $prefix . $token, $tokens);
        $fetched = $this->store->mget($keys);

        return self::decode($this->codec, $plan, $state, $tokens, $prefix, $fetched);
    }

    /** @param list<string> $tokens
     * @return array<string, \stdClass>|null
     */
    private function fetchDatabaseRows(QueryBuilder $query, QueryPlan $plan, array $tokens, bool $allowMissing = false): ?array
    {
        $values = [];
        $nativeIntegersOnly = true;
        foreach ($tokens as $token) {
            $value = $plan->primaryKey->valueFromToken($token);
            if ($value === null) {
                return null;
            }
            $values[] = $value;
            $nativeIntegersOnly = $nativeIntegersOnly && is_int($value);
        }

        $rows = [];
        $mysql = in_array($query->getConnection()->getDriverName(), ['mysql', 'mariadb'], true);
        // SQLite's traditional binding limit is 999; MySQL benefits from larger batches.
        foreach (array_chunk($values, $mysql ? 5000 : 900) as $batch) {
            $builder = $query->getConnection()->query()->from($plan->root->qualifiedTable());
            if ($mysql && $nativeIntegersOnly) {
                $builder->whereIntegerInRaw($plan->primaryKey->column, $batch);
            } else {
                $builder->whereIn($plan->primaryKey->column, $batch);
            }
            foreach ($builder->useWritePdo()->get() as $row) {
                if (!property_exists($row, $plan->primaryKey->column)) {
                    return null;
                }
                $token = $plan->primaryKey->token($row->{$plan->primaryKey->column});
                if ($token === null) {
                    return null;
                }
                $rows[$token] = $row;
            }
        }

        return $allowMissing || array_diff($tokens, array_keys($rows)) === [] ? $rows : null;
    }
}
