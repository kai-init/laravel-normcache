<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\OverlayAdmission;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final readonly class ResultOverlayPublisher
{
    private const MAX_AUTO_OVERLAY_BYTES = 128 * 1024;

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
            $encoded = $this->encodeWithinLimits($rows, $sourceState);

            if ($encoded === null) {
                return false;
            }

            $ttl = $query->configuredTtl() ?? $this->config->queryTtl;
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
                entryKeys: [$resultState->key],
                entryPayloads: [$encoded],
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
                }
            }

            $this->runtime->fail($exception);

            return false;
        }
    }

    public function inlineEntry(
        TableIdentity $root,
        CacheState $sourceState,
        string $namespace,
        string $queryHash,
        array $rows,
    ): OverlayAdmission {
        if ($this->config->maxAutoOverlayRows === 0) {
            return OverlayAdmission::notAttempted();
        }

        try {
            $encoded = $this->encodeWithinLimits($rows, $sourceState);
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return OverlayAdmission::notAttempted();
        }

        if ($encoded === null) {
            return OverlayAdmission::rejected();
        }

        return OverlayAdmission::accepted(
            $this->keys->result($root, $sourceState->version, $namespace, $queryHash),
            $encoded,
        );
    }

    public function rebuildOutcome(CacheRead $read, bool $promoted): CacheRead
    {
        return $promoted
            ? $read->asRepaired('result_overlay_rebuilt')
            : $read->withReason('corrupt_result_overlay_fallback');
    }

    /**
     * @param  array<int, mixed>  $rows
     * @return string|null null when the overlay exceeds the configured row or byte budget
     */
    private function encodeWithinLimits(array $rows, CacheState $state): ?string
    {
        $count = count($rows);

        if (
            $this->config->maxAutoOverlayRows === 0
            || $count > $this->config->maxAutoOverlayRows + self::PAGINATION_LOOKAHEAD_ROWS
        ) {
            return null;
        }

        if ($this->exceedsEstimate($rows, $state, $count)) {
            return null;
        }

        $encoded = $this->codec->encode(
            $rows,
            $state->epoch,
            $state->versions,
            $state->tag,
        );

        return strlen($encoded) > self::MAX_AUTO_OVERLAY_BYTES ? null : $encoded;
    }

    private function exceedsEstimate(array $rows, CacheState $state, int $count): bool
    {
        $first = $this->encodedLength($rows, $state, 1);
        $marginal = $count < 2 ? 0 : $this->encodedLength($rows, $state, 2) - $first;

        return $first + $marginal * ($count - 1) > self::MAX_AUTO_OVERLAY_BYTES;
    }

    private function encodedLength(array $rows, CacheState $state, int $take): int
    {
        return strlen($this->codec->encode(
            array_slice($rows, 0, $take),
            $state->epoch,
            $state->versions,
            $state->tag,
        ));
    }
}
