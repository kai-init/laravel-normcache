<?php

namespace NormCache\Cache;

use Illuminate\Contracts\Container\Container;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisProtocol;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableQueryReply;

final readonly class QueryEntryRepository
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private CacheStateResolver $states,
        private RawResultCodec $codec,
        private MembershipCodec $memberships,
        private Container $container,
    ) {}

    public function read(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        QueryHashResolver $hash,
        bool $claim = false,
    ): ?CacheRead {
        if ($plan->isDirectPrimaryKey()) {
            return $this->readDirect($plan, $namespace, $hash);
        }

        $queryHash = $hash->value();
        if ($plan->isQueryGroup()) {
            [$raw, $values] = $this->store->readHashFieldWithValues(
                $this->keys->queryGroupEntry($queryHash, $namespace),
                'r',
                $this->states->pendingStateKeys($plan, $namespace),
            );

            return $this->readResult($this->states->resolve($plan, $namespace, $queryHash, prefetched: $values), $raw);
        }

        $token = $claim ? hash('xxh128', random_bytes(16)) : null;
        $entry = RedisProtocol::decodeTableQuery($this->store->fetchTableQuery(
            $this->keys->version($plan->root),
            $this->keys->generation($plan->root),
            $this->keys->tablePrefix($plan->root),
            $namespace,
            $queryHash,
            $plan->isCanonical() ? ($this->config->maxAutoOverlayRows > 0 ? 'prefer-result' : 'membership') : 'result',
            $token,
            $token === null ? 0 : $this->config->buildingLockTtl,
        ));
        $lease = $entry->kind === RedisProtocol::MISS && $entry->ownerToken !== null
            ? new BuildLease(
                $entry->isOwner,
                $this->keys->queryBuild($plan->root, $entry->version, $namespace, $queryHash),
                $this->keys->wake($plan->root, 'q', $queryHash, $entry->ownerToken),
                $entry->ownerToken,
            )
            : null;

        try {
            if ($entry->kind === RedisProtocol::MEMBERSHIP) {
                return $this->readCanonical($query, $plan, $namespace, $queryHash, $entry);
            }
            $state = $this->states->resolve(
                $plan,
                $namespace,
                $queryHash,
                $entry->version,
                $entry->generation,
                usesGeneration: $plan->isCanonical() && $entry->kind !== RedisProtocol::RESULT,
            );
            if ($entry->kind === RedisProtocol::MISS) {
                return new CacheRead($state, ReadOutcome::MISS, lease: $lease);
            }

            $result = $this->readResult($state, $entry->payload);
            if (!$plan->isCanonical()) {
                return $result;
            }

            return $result->served()
                ? $result->withReason('result_overlay')
                : new CacheRead($this->states->resolve($plan, $namespace, $queryHash), ReadOutcome::MISS, reason: $result->reason);
        } catch (\Throwable $exception) {
            if ($lease !== null) {
                $this->leases()->release($lease);
            }

            throw $exception;
        }
    }

    private function readDirect(QueryPlan $plan, string $namespace, QueryHashResolver $hash): ?CacheRead
    {
        $reply = $this->runtime->needsRefresh()
            ? $this->store->fetchRowWithRuntime(
                $this->keys->epoch(),
                $this->keys->disabled(),
                $this->keys->generation($plan->root),
                $this->keys->tablePrefix($plan->root),
                (string) $plan->primaryKeyToken,
            )
            : null;
        if ($reply !== null) {
            $this->runtime->rememberState(RedisProtocol::version($reply, 0), RedisProtocol::value($reply, 1) === '1');
            $generation = RedisProtocol::version($reply, 2);
            $raw = RedisProtocol::value($reply, 3);
        } else {
            $reply = $this->store->fetchRow(
                $this->keys->generation($plan->root),
                $this->keys->tablePrefix($plan->root),
                (string) $plan->primaryKeyToken,
            );
            $generation = RedisProtocol::version($reply, 0);
            $raw = RedisProtocol::value($reply, 1);
        }
        if (!$this->runtime->readable()) {
            return null;
        }

        $epoch = $this->runtime->epoch();
        $payload = is_string($raw) ? $this->codec->decodeRow($raw, $plan->primaryKey, $plan->primaryKeyToken) : null;
        $reason = $payload !== null && !$payload->valid ? 'corrupt_payload' : null;
        $row = $payload?->valid && $payload->epoch === $epoch ? $payload->rows[0] : null;
        if ($row !== null && $plan->softDeleteMode !== null && $plan->deletedAtColumn !== null
            && !property_exists($row, $plan->deletedAtColumn)) {
            $row = null;
            $reason = 'corrupt_payload';
        }
        if ($row === null) {
            return new CacheRead(
                $this->states->resolve($plan, $namespace, $hash->value(), knownGeneration: $generation),
                ReadOutcome::MISS,
                reason: $reason,
            );
        }

        $visible = $plan->softDeleteMode === null || $plan->deletedAtColumn === null
            || match ($plan->softDeleteMode) {
                'default' => $row->{$plan->deletedAtColumn} === null,
                'only' => $row->{$plan->deletedAtColumn} !== null,
                default => true,
            };

        return new CacheRead(
            new CacheState(
                $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
                $epoch,
                '0',
                $generation,
                [],
                null,
                null,
            ),
            ReadOutcome::HIT,
            $visible ? [$row] : [],
        );
    }

    private function readCanonical(
        QueryBuilder $query,
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        TableQueryReply $entry,
    ): CacheRead {
        $membership = $this->memberships->decode((string) $entry->payload);
        if (!$membership->valid) {
            return new CacheRead(
                $this->states->resolve($plan, $namespace, $queryHash, $entry->version, $entry->generation),
                ReadOutcome::MISS,
                reason: 'corrupt_payload',
            );
        }

        $generation = $entry->generation ?? '0';
        $prefix = $this->keys->rowPrefix($plan->root, $generation);
        $rowKeys = [];
        foreach ($membership->ids as $token) {
            $rowKeys[$token] = $prefix . $token;
        }
        $fetched = $this->store->mget([
            ...array_values($rowKeys),
            ...$this->states->pendingStateKeys($plan, $namespace),
        ]);
        $state = $this->states->resolve($plan, $namespace, $queryHash, prefetched: $fetched);
        if (
            $state->version !== $entry->version
            || $state->generation !== $generation
            || $membership->epoch !== $state->epoch
            || $membership->generation !== $state->generation
            || $membership->versions !== $state->versions
            || $membership->tagVersion !== $state->tag
        ) {
            return new CacheRead($state, ReadOutcome::MISS);
        }

        [$rows, $missing, $corrupt] = RowRepairer::decode(
            $this->codec,
            $plan,
            $state,
            $membership->ids,
            $prefix,
            $fetched,
        );

        return $missing === []
            ? new CacheRead($state, ReadOutcome::HIT, array_values($rows))
            : $this->container->make(RowRepairer::class)->assemble($query, $plan, $state, $rows, $missing, $corrupt);
    }

    private function readResult(CacheState $state, mixed $raw): CacheRead
    {
        if (!is_string($raw)) {
            return new CacheRead($state, ReadOutcome::MISS);
        }
        $payload = $this->codec->decode($raw);
        if (!$payload->valid) {
            return new CacheRead($state, ReadOutcome::MISS, reason: 'corrupt_payload');
        }

        return $payload->epoch === $state->epoch && $payload->versions === $state->versions && $payload->tagVersion === $state->tag
            ? new CacheRead($state, ReadOutcome::HIT, $payload->rows)
            : new CacheRead($state, ReadOutcome::MISS);
    }

    /** @param array<int, mixed> $rows
     * @param  array<string, true>  $reusedTokens
     */
    public function publish(
        QueryBuilder $query,
        QueryPlan $plan,
        CacheState $state,
        array $rows,
        BuildLease $lease,
        array $reusedTokens = [],
    ): bool {
        $canonical = $plan->isCanonical();
        $direct = $plan->isDirectPrimaryKey();
        $encoded = $canonical || $direct
            ? $this->container->make(RowRepairer::class)->encode($plan, $state, $rows, $reusedTokens)
            : $this->codec->encode($rows, $state->epoch, $state->versions, $state->tag);
        if ($encoded === null || ($direct && $encoded['ids'] !== [$plan->primaryKeyToken])) {
            $this->leases()->release($lease);

            return false;
        }

        if ($canonical) {
            return $this->store->publishCanonical(
                versionKey: $this->keys->version($plan->root),
                generationKey: $this->keys->generation($plan->root),
                membershipKey: $state->key,
                rowKeys: $encoded['rowKeys'],
                rowPayloads: $encoded['rowPayloads'],
                expectedVersion: $state->version,
                expectedGeneration: $state->generation,
                membershipPayload: $this->memberships->encode($state->epoch, $state->generation, $encoded['ids'], $state->versions, $state->tag),
                membershipTtl: $query->configuredTtl() ?? $this->config->queryTtl,
                rowTtl: $this->config->rowTtl,
                buildingKey: $lease->buildingKey,
                wakeKey: (string) $lease->wakeKey,
                token: (string) $lease->token,
                wakeTtl: $this->config->wakeTtl(),
                resultPayload: $this->inlineResult($state, $rows),
            );
        }

        $versionKeys = $plan->isQueryGroup() ? [] : [$this->keys->version($plan->root)];
        $versions = $plan->isQueryGroup() ? [] : [$state->version];
        if ($direct) {
            $versionKeys[] = $this->keys->generation($plan->root);
            $versions[] = $state->generation;
        }

        return $this->store->publishVersionedEntries(
            entryKeys: $direct ? $encoded['rowKeys'] : [$state->key],
            entryPayloads: $direct ? $encoded['rowPayloads'] : [$encoded],
            ttl: $direct ? $this->config->rowTtl : ($query->configuredTtl() ?? $this->config->queryTtl),
            versionKeys: $versionKeys,
            expectedVersions: $versions,
            buildingKey: $lease->buildingKey,
            wakeKey: $lease->wakeKey,
            token: $lease->token,
            wakeTtl: $this->config->wakeTtl(),
            entryFields: $direct ? [] : ['r'],
        );
    }

    private function inlineResult(CacheState $state, array $rows): ?string
    {
        if ($this->config->maxAutoOverlayRows === 0 || count($rows) > $this->config->maxAutoOverlayRows + 1) {
            return null;
        }
        $encoded = $this->codec->encode($rows, $state->epoch, $state->versions, $state->tag);
        $limit = $state->versions === [] && $state->tagKey === null ? 32 * 1024 : 128 * 1024;

        return $encoded !== null && strlen($encoded) <= $limit ? $encoded : null;
    }

    private function leases(): BuildLeaseCoordinator
    {
        return $this->container->make(BuildLeaseCoordinator::class);
    }
}
