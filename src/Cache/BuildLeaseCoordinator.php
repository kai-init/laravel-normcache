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
        $buildingKey = match ($plan->route) {
            QueryPlan::CANONICAL => $this->keys->membershipBuild(
                $plan->root,
                $state->version,
                $namespace,
                $queryHash,
            ),
            QueryPlan::RESULT => $this->keys->resultBuild(
                $plan->root,
                $state->version,
                $namespace,
                $queryHash,
            ),
            QueryPlan::DIRECT_PK => $this->keys->rowBuild(
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

    public function claimRepair(TableIdentity $root, string $repairHash): BuildLease
    {
        return $this->acquire(
            $this->keys->repairBuild($root, $repairHash),
            fn(string $token): string => $this->keys->repairWake($root, $repairHash, $token),
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

        if ($this->store->setNxEx($buildingKey, $token, $this->config->buildingLockTtl)) {
            return new BuildLease(true, $buildingKey, $wakeKey($token), $token);
        }

        $owner = $this->store->getRaw($buildingKey);

        return new BuildLease(
            false,
            $buildingKey,
            is_string($owner) ? $wakeKey($owner) : null,
            $owner,
        );
    }

    private function wakeKey(QueryPlan $plan, string $queryHash, string $token): string
    {
        return match ($plan->route) {
            QueryPlan::CANONICAL => $this->keys->wake($plan->root, 'm', $queryHash, $token),
            QueryPlan::RESULT => $this->keys->wake($plan->root, 'e', $queryHash, $token),
            QueryPlan::DIRECT_PK => $this->keys->wake(
                $plan->root,
                'r',
                (string) $plan->primaryKeyToken,
                $token,
            ),
            default => $this->keys->queryGroupWake($queryHash, $token),
        };
    }
}
