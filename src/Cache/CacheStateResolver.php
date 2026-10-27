<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;

final readonly class CacheStateResolver
{
    public function __construct(
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
    ) {}

    /** @return list<string> */
    public function pendingStateKeys(QueryPlan $plan, string $namespace): array
    {
        return array_values($this->stateKeys(
            $plan,
            $this->tagKey($namespace),
            $this->runtime->knownEpoch() === null,
        ));
    }

    /** @param ?array<string, ?string> $prefetched */
    public function resolve(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        ?string $knownVersion = null,
        ?string $knownGeneration = null,
        ?bool $usesGeneration = null,
        ?array $prefetched = null,
    ): CacheState {
        $usesGeneration ??= $plan->usesGeneration();
        $tagKey = $this->tagKey($namespace);
        if (
            $knownVersion !== null
            && (!$usesGeneration || $knownGeneration !== null)
            && !$plan->isQueryGroup()
            && !$plan->isDirectPrimaryKey()
            && count($plan->dependencies) === 1
            && $plan->dependencies[0]->hash === $plan->root->hash
            && $tagKey === null
            && $prefetched === null
            && $this->runtime->knownEpoch() !== null
        ) {
            return new CacheState(
                $this->keys->queryEntry($plan->root, $knownVersion, $namespace, $queryHash),
                $this->runtime->epoch(),
                $knownVersion,
                $knownGeneration ?? '0',
                [],
                null,
                null,
            );
        }

        $keys = $this->stateKeys($plan, $tagKey, $this->runtime->knownEpoch() === null, $usesGeneration);
        $pending = $keys;
        if ($knownVersion !== null) {
            unset($pending[$plan->root->hash]);
        }
        if ($knownGeneration !== null) {
            unset($pending['generation']);
        }
        $values = $prefetched ?? $this->store->mget(array_values($pending));
        if (isset($keys['epoch'])) {
            $this->runtime->rememberEpoch($values[$keys['epoch']] ?? '0');
        }

        $version = $knownVersion ?? $values[$keys[$plan->root->hash]] ?? '0';
        $generation = $knownGeneration ?? (isset($keys['generation']) ? ($values[$keys['generation']] ?? '0') : '0');
        $versions = [];
        foreach ($plan->dependencies as $dependency) {
            if ($dependency->hash !== $plan->root->hash || $plan->isQueryGroup()) {
                $versions[$dependency->hash] = $dependency->hash === $plan->root->hash
                    ? $version
                    : ($values[$keys[$dependency->hash]] ?? '0');
            }
        }
        ksort($versions, SORT_STRING);

        $key = match (true) {
            $plan->isDirectPrimaryKey() => $this->keys->row($plan->root, $generation, (string) $plan->primaryKeyToken),
            $plan->isQueryGroup() => $this->keys->queryGroupEntry($queryHash, $namespace),
            default => $this->keys->queryEntry($plan->root, $version, $namespace, $queryHash),
        };

        return new CacheState(
            $key,
            $this->runtime->epoch(),
            $version,
            $generation,
            $versions,
            $tagKey === null ? null : ($values[$tagKey] ?? '0'),
            $tagKey,
        );
    }

    /** @phpstan-impure */
    public function isCurrent(QueryPlan $plan, CacheState $expected): bool
    {
        return $this->isCurrentFrom($plan, $expected, $this->store->mget($this->validationKeys($plan, $expected)));
    }

    /** @return list<string> */
    public function validationKeys(QueryPlan $plan, CacheState $expected): array
    {
        return array_values($this->stateKeys($plan, $expected->tagKey, true, true));
    }

    /** @param array<string, ?string> $values */
    public function isCurrentFrom(QueryPlan $plan, CacheState $expected, array $values): bool
    {
        foreach ($this->stateKeys($plan, $expected->tagKey, true, true) as $kind => $key) {
            $version = match ($kind) {
                'epoch' => $expected->epoch,
                'generation' => $expected->generation,
                'tag' => $expected->tag,
                $plan->root->hash => $expected->version,
                default => $expected->versions[$kind] ?? null,
            };
            if (($values[$key] ?? '0') !== $version) {
                return false;
            }
        }

        return true;
    }

    /** @return array<string, string> */
    private function stateKeys(
        QueryPlan $plan,
        ?string $tagKey,
        bool $epoch,
        ?bool $generation = null,
    ): array {
        $keys = [$plan->root->hash => $this->keys->version($plan->root)];
        foreach ($plan->dependencies as $dependency) {
            if ($dependency->hash !== $plan->root->hash) {
                $keys[$dependency->hash] = $this->keys->version($dependency);
            }
        }
        if ($generation ?? $plan->usesGeneration()) {
            $keys['generation'] = $this->keys->generation($plan->root);
        }
        if ($epoch) {
            $keys['epoch'] = $this->keys->epoch();
        }
        if ($tagKey !== null) {
            $keys['tag'] = $tagKey;
        }

        return $keys;
    }

    private function tagKey(string $namespace): ?string
    {
        return str_starts_with($namespace, 'g')
            ? $this->keys->tagVersion(substr($namespace, 1, 32))
            : null;
    }
}
