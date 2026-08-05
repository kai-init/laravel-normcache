<?php

namespace NormCache\Cache;

use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisProtocol;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CachedRow;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;

final readonly class CanonicalRowRepository
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private RawResultCodec $codec,
        private BuildLeaseCoordinator $leases,
    ) {}

    public function read(QueryPlan $plan): CachedRow
    {
        $result = $this->store->fetchRow(
            $this->keys->generation($plan->root),
            $this->keys->tablePrefix($plan->root),
            (string) $plan->primaryKeyToken,
        );
        $generation = RedisProtocol::version($result, 0);
        $raw = RedisProtocol::value($result, 1);

        if (!is_string($raw)) {
            return new CachedRow($generation);
        }

        $payload = $this->codec->decodeRow(
            $raw,
            $plan->primaryKey,
            $plan->primaryKeyToken,
        );

        if (!$payload->valid) {
            return new CachedRow($generation, reason: 'corrupt_payload');
        }

        $epoch = $this->runtime->epoch();

        if ($payload->epoch !== $epoch) {
            return new CachedRow($generation);
        }

        return new CachedRow($generation, $epoch, $payload->rows[0]);
    }

    public function state(QueryPlan $plan, string $generation, string $epoch): CacheState
    {
        return new CacheState(
            key: $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
            epoch: $epoch,
            version: '0',
            generation: $generation,
            versions: [],
            tag: null,
            tagKey: null,
        );
    }

    /** @return list<\stdClass>|null null on missing deleted-at column; empty array when filtered by visibility. */
    public function visibleRows(QueryPlan $plan, \stdClass $row): ?array
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

    /** @param array<int, mixed> $rows */
    public function publish(
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
            $this->leases->release($lease);

            return;
        }

        $encoded = $this->codec->encodeRow($rows[0], $state->epoch);

        $this->store->publishVersionedEntries(
            entryKeys: [$state->key],
            entryPayloads: [$encoded],
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
            wakeTtl: $this->config->wakeTtl(),
        );
    }
}
