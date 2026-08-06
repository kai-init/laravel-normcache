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

    public function resolve(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        ?string $knownVersion = null,
        ?string $knownGeneration = null,
        ?bool $usesGeneration = null,
    ): CacheState {
        $versionKeys = [];

        foreach ($plan->dependencies as $dependency) {
            $versionKeys[$dependency->hash] = $this->keys->version($dependency);
        }

        $rootVersionKey = $versionKeys[$plan->root->hash] ?? null;

        if ($knownVersion !== null && $rootVersionKey !== null) {
            unset($versionKeys[$plan->root->hash]);
        }

        $generationKey = $knownGeneration === null && ($usesGeneration ?? $plan->usesGeneration())
            ? $this->keys->generation($plan->root)
            : null;
        $tagKey = $this->tagKey($namespace);
        $epochKey = $this->unknownEpochKey();
        $values = $this->store->mget(array_values(array_unique(array_filter([
            $epochKey,
            ...array_values($versionKeys),
            $generationKey,
            $tagKey,
        ]))));
        $this->rememberEpochFrom($epochKey, $values);

        if (
            $knownVersion !== null
            && $rootVersionKey !== null
            && !array_key_exists($rootVersionKey, $values)
        ) {
            $versionKeys[$plan->root->hash] = $rootVersionKey;
            $values[$rootVersionKey] = $knownVersion;
        }

        $allVersions = [];

        foreach ($versionKeys as $hash => $key) {
            $allVersions[$hash] = $values[$key] ?? '0';
        }

        ksort($allVersions, SORT_STRING);
        $rootVersion = $knownVersion ?? $allVersions[$plan->root->hash] ?? '0';
        $generation = $knownGeneration
            ?? ($generationKey !== null ? ($values[$generationKey] ?? '0') : '0');
        $tag = $tagKey !== null ? ($values[$tagKey] ?? '0') : null;
        $versions = $allVersions;

        if (!$plan->isQueryGroup()) {
            unset($versions[$plan->root->hash]);
        }

        $key = match (true) {
            $plan->isDirectPrimaryKey() => $this->keys->row(
                $plan->root,
                $generation,
                (string) $plan->primaryKeyToken,
            ),
            $plan->isQueryGroup() => $this->keys->queryGroupEntry($queryHash, $namespace),
            default => $this->keys->queryEntry(
                $plan->root,
                $rootVersion,
                $namespace,
                $queryHash,
            ),
        };

        return new CacheState(
            key: $key,
            epoch: $this->runtime->epoch(),
            version: $rootVersion,
            generation: $generation,
            versions: $versions,
            tag: $tag,
            tagKey: $tagKey,
        );
    }

    /**
     * @param  list<string>  $rowKeys
     * @return array{0: CacheState, 1: array<string, ?string>}
     */
    public function resolveCanonical(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $rowKeys,
    ): array {
        $keys = $this->stateKeys(
            $plan,
            $this->tagKey($namespace),
            $this->unknownEpochKey(),
            usesGeneration: true,
        );
        $values = $this->store->mget(array_values(array_unique([
            ...$rowKeys,
            ...$keys['all'],
        ])));
        $this->rememberEpochFrom($keys['epoch'], $values);

        $versions = [];

        foreach ($keys['dependencies'] as $hash => $key) {
            $versions[$hash] = $values[$key] ?? '0';
        }

        ksort($versions, SORT_STRING);
        $version = $values[$keys['version']] ?? '0';

        return [
            new CacheState(
                key: $this->keys->queryEntry($plan->root, $version, $namespace, $queryHash),
                epoch: $this->runtime->epoch(),
                version: $version,
                generation: $keys['generation'] !== null
                    ? ($values[$keys['generation']] ?? '0')
                    : '0',
                versions: $versions,
                tag: $keys['tag'] !== null ? ($values[$keys['tag']] ?? '0') : null,
                tagKey: $keys['tag'],
            ),
            $values,
        ];
    }

    /** @phpstan-impure */
    public function isCurrent(
        QueryPlan $plan,
        CacheState $expected,
        ?bool $usesGeneration = null,
    ): bool {
        $keys = $this->stateKeys(
            $plan,
            $expected->tagKey,
            $this->keys->epoch(),
            $usesGeneration,
        );
        $values = $this->store->mget($keys['all']);
        $current = static fn(string $key): string => $values[$key] ?? '0';

        if (
            $current((string) $keys['epoch']) !== $expected->epoch
            || $current($keys['version']) !== $expected->version
            || $keys['generation'] !== null
                && $current($keys['generation']) !== $expected->generation
        ) {
            return false;
        }

        foreach ($keys['dependencies'] as $hash => $key) {
            if ($current($key) !== ($expected->versions[$hash] ?? null)) {
                return false;
            }
        }

        return $expected->tag === null
            || $keys['tag'] !== null && $current($keys['tag']) === $expected->tag;
    }

    /**
     * @return array{
     *     epoch: ?string,
     *     version: string,
     *     generation: string|null,
     *     dependencies: array<string, string>,
     *     tag: ?string,
     *     all: list<string>
     * }
     */
    private function stateKeys(
        QueryPlan $plan,
        ?string $tagKey,
        ?string $epochKey,
        ?bool $usesGeneration = null,
    ): array {
        $dependencies = [];

        foreach ($plan->dependencies as $dependency) {
            if ($dependency->hash !== $plan->root->hash) {
                $dependencies[$dependency->hash] = $this->keys->version($dependency);
            }
        }

        $versionKey = $this->keys->version($plan->root);
        $generationKey = ($usesGeneration ?? $plan->usesGeneration())
            ? $this->keys->generation($plan->root)
            : null;

        return [
            'epoch' => $epochKey,
            'version' => $versionKey,
            'generation' => $generationKey,
            'dependencies' => $dependencies,
            'tag' => $tagKey,
            'all' => array_values(array_filter([
                $versionKey,
                $generationKey,
                $epochKey,
                ...array_values($dependencies),
                $tagKey,
            ])),
        ];
    }

    private function tagKey(string $namespace): ?string
    {
        return str_starts_with($namespace, 'g')
            ? $this->keys->tagVersion(substr($namespace, 1, 32))
            : null;
    }

    private function unknownEpochKey(): ?string
    {
        return $this->runtime->knownEpoch() === null ? $this->keys->epoch() : null;
    }

    /** @param array<string, ?string> $values */
    private function rememberEpochFrom(?string $epochKey, array $values): void
    {
        if ($epochKey !== null) {
            $this->runtime->rememberEpoch($values[$epochKey] ?? '0');
        }
    }
}
