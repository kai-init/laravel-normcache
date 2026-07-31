<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheState;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class ConcurrencyTest extends TestCase
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

    public function test_a_request_that_loses_the_lease_race_serves_from_the_database(): void
    {
        Author::create(['name' => 'Alice']);
        $query = static fn() => Author::orderBy('id')->get();

        // The build lease only exists between the claim and the publish, so the
        // key is captured from inside that window — while the SQL that the lease
        // is protecting is running.
        $buildKey = null;
        DB::listen(function () use (&$buildKey): void {
            $buildKey ??= $this->cacheKeysMatching(':build:')[0] ?? null;
        });

        $query();
        $this->assertIsString($buildKey, 'expected a build lease to be held across the database read');

        // Drop the published payloads but keep the version and generation counters:
        // the build key embeds the version, so resetting it would make the planted
        // lease refer to a key the next request never looks at.
        $this->cacheStore()->delete([
            ...$this->cacheKeysMatching(':m:v'),
            ...$this->cacheKeysMatching(':e:v'),
            ...$this->cacheKeysMatching(':r:g'),
        ]);

        // Stand in for another node that claimed the lease and has not published yet.
        $this->cacheStore()->setNxEx($buildKey, str_repeat('f', 32), 5);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $contended = $query();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertCount(1, $contended);
        $this->assertSame('Alice', $contended->first()->name);
        $this->assertNotSame([], $queries, 'a losing claimant must fall through to the database');
        $published = array_values(array_filter(
            $this->cacheKeysMatching(':m:v'),
            static fn(string $key): bool => !str_contains($key, ':build:'),
        ));

        $this->assertSame([], $published, 'a losing claimant must not publish');
    }

    public function test_a_waiter_stops_waiting_as_soon_as_the_owner_wakes_it(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $plan = $this->plan();

        $owner = $leases->claim($plan, $this->state(), 'n', 'woken');
        $waiter = $leases->claim($plan, $this->state(), 'n', 'woken');

        $this->assertTrue($owner->owner);
        $this->assertFalse($waiter->owner);
        $this->assertSame($owner->wakeKey, $waiter->wakeKey);

        $leases->release($owner);

        // Released with waiters pending, so the wake list is already populated and
        // the blocking pop returns without burning the stampede timeout.
        $started = hrtime(true);
        $woken = $this->cacheStore()->brpop((string) $waiter->wakeKey, 2.0);
        $elapsedMs = (hrtime(true) - $started) / 1e6;

        $this->assertTrue($woken);
        $this->assertLessThan(1000, $elapsedMs);
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
