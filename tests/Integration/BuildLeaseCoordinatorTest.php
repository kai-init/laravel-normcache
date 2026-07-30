<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheState;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class BuildLeaseCoordinatorTest extends TestCase
{
    public function test_the_first_claimant_owns_the_lease_and_the_second_does_not(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $plan = $this->plan();

        $first = $leases->claim($plan, $this->state(), 'n', 'hash');
        $second = $leases->claim($plan, $this->state(), 'n', 'hash');

        $this->assertTrue($first->owner);
        $this->assertFalse($second->owner);
        $this->assertSame($first->buildingKey, $second->buildingKey);
    }

    public function test_a_waiter_learns_the_owner_wake_key(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $plan = $this->plan();

        $owner = $leases->claim($plan, $this->state(), 'n', 'hash');
        $waiter = $leases->claim($plan, $this->state(), 'n', 'hash');

        $this->assertSame($owner->wakeKey, $waiter->wakeKey);
        $this->assertSame($owner->token, $waiter->token);
    }

    public function test_releasing_a_lease_lets_the_next_caller_claim_it(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $plan = $this->plan();

        $first = $leases->claim($plan, $this->state(), 'n', 'hash');
        $leases->release($first);

        $this->assertTrue($leases->claim($plan, $this->state(), 'n', 'hash')->owner);
    }

    public function test_releasing_a_lease_this_caller_does_not_own_is_a_no_op(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $plan = $this->plan();

        $owner = $leases->claim($plan, $this->state(), 'n', 'hash');
        $waiter = $leases->claim($plan, $this->state(), 'n', 'hash');

        $leases->release($waiter);

        $this->assertFalse($leases->claim($plan, $this->state(), 'n', 'hash')->owner);
        $this->assertTrue($owner->owner);
    }

    public function test_repair_leases_use_the_repair_key_family_and_share_the_protocol(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $root = $this->table();

        $first = $leases->claimRepair($root, 'repair-hash');
        $second = $leases->claimRepair($root, 'repair-hash');

        $this->assertTrue($first->owner);
        $this->assertFalse($second->owner);
        $this->assertSame($first->wakeKey, $second->wakeKey);
        $this->assertNotSame(
            $first->buildingKey,
            $leases->claim($this->plan(), $this->state(), 'n', 'repair-hash')->buildingKey,
        );

        $leases->release($first);

        $this->assertTrue($leases->claimRepair($root, 'repair-hash')->owner);
    }

    public function test_distinct_repair_hashes_do_not_share_a_lease(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $root = $this->table();

        $this->assertTrue($leases->claimRepair($root, 'hash-a')->owner);
        $this->assertTrue($leases->claimRepair($root, 'hash-b')->owner);
    }

    private function plan(): QueryPlan
    {
        $root = $this->table();

        return QueryPlan::canonical(
            $root,
            [$root],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            materializeResult: false,
        );
    }

    private function state(): CacheState
    {
        return new CacheState(
            key: 'k',
            epoch: '1',
            version: '1',
            generation: '1',
            versions: [],
            tag: null,
            tagKey: null,
        );
    }

    private function table(): TableIdentity
    {
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);

        return $table;
    }
}
