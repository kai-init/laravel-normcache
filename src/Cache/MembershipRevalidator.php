<?php

namespace NormCache\Cache;

use NormCache\Enums\MutationType;
use NormCache\Payload\ChangeRecordCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheRead;

final readonly class MembershipRevalidator
{
    private const MAX_VERSION_GAP = 128;

    public function __construct(
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private ChangeRecordCodec $changes,
    ) {}

    public function revalidate(ReadContext $context, CacheRead $read): bool
    {
        $stale = $read->staleMembership;
        $predicate = $context->plan->predicateColumns;

        // Projections rebuild from repaired canonical rows.
        if (
            $stale === null
            || $predicate === null
            || !($context->plan->isCanonical() || $context->plan->supportsCanonicalProjectionFallback())
        ) {
            return false;
        }

        $from = $stale->rootVersion;
        $to = $read->state->version;

        if ($from === null || !ctype_digit($from) || !ctype_digit($to)) {
            return false;
        }

        $from = (int) $from;
        $to = (int) $to;

        if ($to <= $from || ($to - $from) > self::MAX_VERSION_GAP) {
            return false;
        }

        // Change records describe only root-version gaps.
        if (
            $stale->epoch !== $read->state->epoch
            || $stale->generation !== $read->state->generation
            || $stale->versions !== $read->state->versions
            || $stale->tagVersion !== $read->state->tag
        ) {
            return false;
        }

        $keys = [];

        for ($version = $from + 1; $version <= $to; $version++) {
            $keys[] = $this->keys->changeRecord($context->plan->root, (string) $version);
        }

        $records = $this->store->mget($keys);
        $guarded = array_flip($predicate);

        foreach ($records as $payload) {
            if (!is_string($payload)) {
                return false;
            }

            $record = $this->changes->decode($payload);

            if (
                !$record->valid
                || !$record->precise
                || $record->mutation !== MutationType::UPDATE->value
            ) {
                return false;
            }

            foreach ($record->columns as $column) {
                if (isset($guarded[$column])) {
                    return false;
                }
            }
        }

        return true;
    }
}
