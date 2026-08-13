<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildLease;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheState;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final readonly class BuildLeaseCoordinator
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
    ) {}

    public function claim(
        QueryPlan $plan,
        CacheState $state,
        string $namespace,
        string $queryHash,
    ): BuildLease {
        $buildingKey = match (true) {
            !$plan->isDirectPrimaryKey() && !$plan->isQueryGroup() => $this->keys->queryBuild(
                $plan->root,
                $state->version,
                $namespace,
                $queryHash,
            ),
            $plan->isDirectPrimaryKey() => $this->keys->rowBuild(
                $plan->root,
                $state->generation,
                (string) $plan->primaryKeyToken,
            ),
            default => $this->keys->queryGroupBuild($queryHash),
        };

        return $this->acquire(
            $buildingKey,
            fn(string $token): string => $this->wakeKey($plan, $queryHash, $token),
        );
    }

    public function claimRepair(
        TableIdentity $root,
        string $generation,
        string $batchHash,
    ): BuildLease {
        return $this->acquire(
            $this->keys->repairBuild($root, $generation, $batchHash),
            fn(string $token): string => $this->keys->repairWake(
                $root,
                $generation,
                $batchHash,
                $token,
            ),
        );
    }

    public function release(BuildLease $lease, bool $wakeWaiters = true): void
    {
        if (!$lease->owner || $lease->token === null) {
            return;
        }

        try {
            $this->store->releaseBuilding(
                $lease->buildingKey,
                $wakeWaiters ? (string) $lease->wakeKey : '',
                $lease->token,
                $this->config->wakeTtl(),
            );
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);
        }
    }

    /** @param callable(string): string $wakeKey */
    private function acquire(string $buildingKey, callable $wakeKey): BuildLease
    {
        $token = bin2hex(random_bytes(16));
        [$owner, $ownerToken] = $this->store->claimBuild(
            $buildingKey,
            $token,
            $this->config->buildingLockTtl,
        );

        return new BuildLease(
            $owner,
            $buildingKey,
            $ownerToken === null ? null : $wakeKey($ownerToken),
            $ownerToken,
        );
    }

    private function wakeKey(QueryPlan $plan, string $queryHash, string $token): string
    {
        return match (true) {
            !$plan->isDirectPrimaryKey() && !$plan->isQueryGroup() => $this->keys->wake(
                $plan->root,
                'q',
                $queryHash,
                $token,
            ),
            $plan->isDirectPrimaryKey() => $this->keys->wake(
                $plan->root,
                'r',
                (string) $plan->primaryKeyToken,
                $token,
            ),
            default => $this->keys->queryGroupWake($queryHash, $token),
        };
    }
}
