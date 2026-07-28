<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\RuntimeState;

final readonly class CacheStateResolver
{
    public function __construct(
        private RuntimeState $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
    ) {}

    /**
     * @param  list<string>  $alsoFetch
     * @return array{0: CacheState, 1: array<string, ?string>}
     */
    public function resolve(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        ?string $knownVersion = null,
        ?string $knownGeneration = null,
        array $alsoFetch = [],
    ): array {
        $versionKeys = [];

        foreach ($plan->dependencies as $dependency) {
            $versionKeys[$dependency->hash] = $this->keys->version($dependency);
        }

        $rootVersionKey = $versionKeys[$plan->root->hash] ?? null;

        if ($knownVersion !== null && $rootVersionKey !== null) {
            unset($versionKeys[$plan->root->hash]);
        }

        $generationKey = $knownGeneration === null ? match ($plan->route) {
            QueryPlan::CANONICAL, QueryPlan::DIRECT_PK => $this->keys->generation($plan->root),
            default => null,
        } : null;
        $tagKey = str_starts_with($namespace, 'g')
            ? $this->keys->tagVersion(substr($namespace, 1))
            : null;
        $epochKey = $this->runtime->knownEpoch() === null ? $this->keys->epoch() : null;
        $values = $this->store->mget(array_values(array_unique(array_filter([
            $epochKey,
            ...array_values($versionKeys),
            $generationKey,
            $tagKey,
            ...$alsoFetch,
        ]))));

        if ($epochKey !== null) {
            $this->runtime->rememberEpoch($values[$epochKey] ?? '0');
        }

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

        if ($plan->route !== QueryPlan::QUERY_GROUP) {
            unset($versions[$plan->root->hash]);
        }

        $key = match ($plan->route) {
            QueryPlan::CANONICAL => $this->keys->membership(
                $plan->root,
                $rootVersion,
                $namespace,
                $queryHash,
            ),
            QueryPlan::DIRECT_PK => $this->keys->row(
                $plan->root,
                $generation,
                (string) $plan->primaryKeyToken,
            ),
            QueryPlan::QUERY_GROUP => $this->keys->queryGroupResult($queryHash, $namespace),
            default => $this->keys->result(
                $plan->root,
                $rootVersion,
                $namespace,
                $queryHash,
            ),
        };

        return [
            new CacheState(
                key: $key,
                epoch: $this->epoch(),
                version: $rootVersion,
                generation: $generation,
                versions: $versions,
                tag: $tag,
                tagKey: $tagKey,
            ),
            $values,
        ];
    }

    /**
     * @return array{
     *     epoch: ?string,
     *     version: string,
     *     generation: string,
     *     dependencies: array<string, string>,
     *     tag: ?string,
     *     final: list<string>
     * }
     */
    public function canonicalKeys(QueryPlan $plan, string $namespace): array
    {
        $dependencyKeys = [];

        foreach ($plan->dependencies as $dependency) {
            if ($dependency->hash !== $plan->root->hash) {
                $dependencyKeys[$dependency->hash] = $this->keys->version($dependency);
            }
        }

        $epochKey = $this->runtime->knownEpoch() === null ? $this->keys->epoch() : null;
        $versionKey = $this->keys->version($plan->root);
        $generationKey = $this->keys->generation($plan->root);
        $tagKey = str_starts_with($namespace, 'g')
            ? $this->keys->tagVersion(substr($namespace, 1))
            : null;
        $externalKeys = array_values(array_filter([
            $epochKey,
            ...array_values($dependencyKeys),
            $tagKey,
        ]));

        return [
            'epoch' => $epochKey,
            'version' => $versionKey,
            'generation' => $generationKey,
            'dependencies' => $dependencyKeys,
            'tag' => $tagKey,
            'final' => [$versionKey, $generationKey, ...$externalKeys],
        ];
    }

    /**
     * @param  array{
     *     epoch: ?string,
     *     version: string,
     *     generation: string,
     *     dependencies: array<string, string>,
     *     tag: ?string,
     *     final: list<string>
     * }  $keys
     * @param  array<string, ?string>  $values
     */
    public function canonicalFromFetched(
        QueryPlan $plan,
        string $namespace,
        string $queryHash,
        array $keys,
        array $values,
    ): CacheState {
        if ($keys['epoch'] !== null) {
            $this->runtime->rememberEpoch($values[$keys['epoch']] ?? '0');
        }

        $versions = [];

        foreach ($keys['dependencies'] as $hash => $key) {
            $versions[$hash] = $values[$key] ?? '0';
        }

        ksort($versions, SORT_STRING);
        $version = $values[$keys['version']] ?? '0';
        $generation = $values[$keys['generation']] ?? '0';
        $tag = $keys['tag'] !== null ? ($values[$keys['tag']] ?? '0') : null;

        return new CacheState(
            key: $this->keys->membership(
                $plan->root,
                $version,
                $namespace,
                $queryHash,
            ),
            epoch: $this->epoch(),
            version: $version,
            generation: $generation,
            versions: $versions,
            tag: $tag,
            tagKey: $keys['tag'],
        );
    }

    /**
     * @param  list<string>  $keys
     * @param  list<string>  $alsoFetch
     * @return array<string, ?string>
     */
    public function fetch(array $keys, array $alsoFetch = []): array
    {
        return $this->store->mget(array_values(array_unique([
            ...$keys,
            ...$alsoFetch,
        ])));
    }

    private function epoch(): string
    {
        return $this->runtime->epoch(fn(): string => $this->store->getRaw($this->keys->epoch()) ?? '0');
    }
}
