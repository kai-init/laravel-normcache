<?php

namespace NormCache\Cache;

use NormCache\Enums\ReadOutcome;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisProtocol;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;

final readonly class CacheReader
{
    public function __construct(
        private CacheConfig $config,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private CacheStateResolver $states,
        private QueryEntryRepository $entries,
        private CanonicalRowRepository $rows,
    ) {}

    public function read(ReadContext $context, QueryHashResolver $hash): CacheRead
    {
        if ($context->plan->isDirectPrimaryKey()) {
            return $this->readDirect($context, $hash);
        }

        $queryHash = $hash->value();

        if ($context->plan->isCanonical()) {
            return $this->readCanonical($context, $queryHash);
        }

        if ($context->plan->isQueryGroup()) {
            [$raw, $values] = $this->store->readHashFieldWithValues(
                $this->keys->queryGroupEntry($queryHash, $context->namespace),
                'r',
                $this->states->pendingStateKeys($context->plan, $context->namespace),
            );
            $state = $this->states->resolve(
                $context->plan,
                $context->namespace,
                $queryHash,
                prefetched: $values,
            );
        } else {
            $entry = $this->store->fetchResult(
                $this->keys->version($context->plan->root),
                $this->keys->tablePrefix($context->plan->root),
                $context->namespace,
                $queryHash,
            );
            $raw = RedisProtocol::value($entry, 1);
            $state = $this->states->resolve(
                $context->plan,
                $context->namespace,
                $queryHash,
                RedisProtocol::version($entry, 0),
            );
        }

        return $this->entries->readResult($state, $raw);
    }

    private function readCanonical(ReadContext $context, string $queryHash): CacheRead
    {
        $arguments = [
            $this->keys->version($context->plan->root),
            $this->keys->generation($context->plan->root),
            $this->keys->tablePrefix($context->plan->root),
            $context->namespace,
            $queryHash,
        ];
        $head = $this->config->maxAutoOverlayRows > 0
            ? $this->store->fetchResultOrCanonical(...$arguments)
            : $this->store->fetchCanonical(...$arguments);

        if (RedisProtocol::status($head) === RedisProtocol::RESULT) {
            $state = $this->states->resolve(
                $context->plan,
                $context->namespace,
                $queryHash,
                RedisProtocol::version($head),
                usesGeneration: false,
            );
            $result = $this->entries->readResult($state, RedisProtocol::resultPayload($head));

            return $result->served()
                ? $result->withReason('result_overlay')
                : new CacheRead(
                    $this->states->resolve($context->plan, $context->namespace, $queryHash),
                    ReadOutcome::MISS,
                    [],
                    $result->reason,
                );
        }

        if (RedisProtocol::status($head) === RedisProtocol::MEMBERSHIP) {
            $head[0] = RedisProtocol::HIT;
        }

        return $this->entries->readCanonical($context->query, $context->plan, $context->namespace, $queryHash, $head);
    }

    private function readDirect(ReadContext $context, QueryHashResolver $hash): CacheRead
    {
        $cached = $this->rows->read($context->plan);
        $resolve = fn(): CacheState => $this->states->resolve(
            $context->plan,
            $context->namespace,
            $hash->value(),
            knownGeneration: $cached->generation,
        );

        if ($cached->row === null) {
            return new CacheRead($resolve(), ReadOutcome::MISS, [], $cached->reason);
        }

        $rows = $this->rows->visibleRows($context->plan, $cached->row);

        return $rows === null
            ? new CacheRead($resolve(), ReadOutcome::MISS, [], 'corrupt_payload')
            : new CacheRead(
                $this->rows->state($context->plan, $cached->generation, (string) $cached->epoch),
                ReadOutcome::HIT,
                $rows,
            );
    }
}
