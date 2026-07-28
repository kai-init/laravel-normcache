<?php

namespace NormCache\Cache;

use NormCache\Database\CachingQueryBuilder;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
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

    /** @return array{hit: bool, rows: array, reason: ?string} */
    public function read(CacheState $state, mixed $raw): array
    {
        if (!is_string($raw)) {
            return ['hit' => false, 'rows' => [], 'reason' => null];
        }

        $payload = $this->codec->decode($raw);

        if (!$payload->valid) {
            return ['hit' => false, 'rows' => [], 'reason' => 'corrupt_payload'];
        }

        $hit = $payload->epoch === $state->epoch
            && $payload->versions === $state->versions
            && $payload->tagVersion === $state->tag;

        return ['hit' => $hit, 'rows' => $hit ? $payload->rows : [], 'reason' => null];
    }

    /** @param array<int, mixed> $rows */
    public function publish(
        CachingQueryBuilder $query,
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
            ttl: $query->normCacheTtl() ?? $this->config->queryTtl,
            versionKeys: $versionKeys,
            expectedVersions: $expected,
            buildingKey: $lease->buildingKey,
            wakeKey: $lease->wakeKey,
            token: $lease->token,
            wakeTtl: $wakeTtl,
        );
    }
}
