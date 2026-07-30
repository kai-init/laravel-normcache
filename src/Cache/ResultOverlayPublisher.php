<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;

final readonly class ResultOverlayPublisher
{
    private const MAX_AUTO_OVERLAY_BYTES = 50 * 1024;

    private const PAGINATION_LOOKAHEAD_ROWS = 1;

    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private RawResultCodec $codec,
        private CacheStateResolver $states,
        private BuildLeaseCoordinator $leases,
    ) {}

    /** @param array<int, mixed> $rows */
    public function promote(
        QueryBuilder $query,
        QueryPlan $resultPlan,
        CacheState $sourceState,
        string $namespace,
        string $queryHash,
        array $rows,
        bool $wakeWaiters = true,
    ): bool {
        try {
            if (
                $this->config->maxAutoOverlayRows === 0
                || count($rows) > $this->config->maxAutoOverlayRows + self::PAGINATION_LOOKAHEAD_ROWS
            ) {
                return false;
            }

            $ttl = $query->configuredTtl() ?? $this->config->queryTtl;
            $encoded = $this->codec->encode(
                $rows,
                $sourceState->epoch,
                $sourceState->versions,
                $sourceState->tag,
            );

            if (strlen($encoded) > self::MAX_AUTO_OVERLAY_BYTES) {
                return false;
            }

            $resultState = new CacheState(
                key: $this->keys->result(
                    $resultPlan->root,
                    $sourceState->version,
                    $namespace,
                    $queryHash,
                ),
                epoch: $sourceState->epoch,
                version: $sourceState->version,
                generation: '0',
                versions: $sourceState->versions,
                tag: $sourceState->tag,
                tagKey: $sourceState->tagKey,
            );
            $lease = $this->leases->claim($resultPlan, $resultState, $namespace, $queryHash);

            if (!$lease->owner) {
                return false;
            }

            $current = $this->states->resolve($resultPlan, $namespace, $queryHash)[0];

            if (!$current->equals($resultState)) {
                $this->leases->release($lease, $wakeWaiters);

                return false;
            }

            return $this->store->publishVersionedEntries(
                entries: [$resultState->key => $encoded],
                ttl: $ttl,
                versionKeys: [$this->keys->version($resultPlan->root)],
                expectedVersions: [$resultState->version],
                buildingKey: $lease->buildingKey,
                wakeKey: $wakeWaiters ? $lease->wakeKey : null,
                token: $lease->token,
                wakeTtl: $this->config->wakeTtl(),
            );
        } catch (\Throwable $exception) {
            if (isset($lease) && $lease->owner) {
                try {
                    $this->leases->release($lease, $wakeWaiters);
                } catch (\Throwable) {
                    // The original Redis failure is the useful diagnostic.
                }
            }

            $this->runtime->fail($exception);

            return false;
        }
    }

    public function rebuildOutcome(CacheRead $read, bool $promoted): CacheRead
    {
        return $promoted
            ? $read->asRepaired('result_overlay_rebuilt')
            : $read->withReason('corrupt_result_overlay_fallback');
    }
}
