<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;

final readonly class ResultRepository
{
    public function __construct(
        private CacheConfig $config,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private RawResultCodec $codec,
    ) {}

    public function read(CacheState $state, mixed $raw): CacheRead
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
    public function publish(
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

        $versionKeys = $plan->route === QueryPlan::RESULT
            ? [$this->keys->version($plan->root)]
            : [];
        $expected = $plan->route === QueryPlan::RESULT
            ? [$state->version]
            : [];

        return $this->store->publishVersionedEntries(
            entries: [$state->key => $encoded],
            ttl: $query->configuredTtl() ?? $this->config->queryTtl,
            versionKeys: $versionKeys,
            expectedVersions: $expected,
            buildingKey: $lease->buildingKey,
            wakeKey: $lease->wakeKey,
            token: $lease->token,
            wakeTtl: $wakeTtl,
        );
    }
}
