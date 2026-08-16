<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Cache\CacheRuntime;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\QueryIdentity;
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

    public function test_claiming_with_the_same_token_is_idempotent(): void
    {
        $key = 'test:{claim-build}:lease';
        $token = str_repeat('a', 32);

        [$firstOwner, $firstToken] = $this->cacheStore()->claimBuild($key, $token, 30);
        [$retryOwner, $retryToken] = $this->cacheStore()->claimBuild($key, $token, 30);
        [$otherOwner, $observedToken] = $this->cacheStore()->claimBuild(
            $key,
            str_repeat('b', 32),
            30,
        );

        $this->assertTrue($firstOwner);
        $this->assertSame($token, $firstToken);
        $this->assertTrue($retryOwner);
        $this->assertSame($token, $retryToken);
        $this->assertFalse($otherOwner);
        $this->assertSame($token, $observedToken);
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

    public function test_matching_row_repairs_share_one_lease(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $root = $this->table();

        $owner = $leases->claimRepair($root, '4', 'batch');
        $waiter = $leases->claimRepair($root, '4', 'batch');

        $this->assertTrue($owner->owner);
        $this->assertFalse($waiter->owner);
        $this->assertSame($owner->buildingKey, $waiter->buildingKey);
        $this->assertSame($owner->wakeKey, $waiter->wakeKey);
    }

    public function test_row_repairs_from_different_generations_do_not_share_a_lease(): void
    {
        $leases = $this->app->make(BuildLeaseCoordinator::class);
        $root = $this->table();

        $this->assertTrue($leases->claimRepair($root, '4', 'batch')->owner);
        $this->assertTrue($leases->claimRepair($root, '5', 'batch')->owner);
    }

    public function test_a_request_that_loses_the_lease_race_serves_from_the_database(): void
    {
        Author::create(['name' => 'Alice']);
        $query = static fn() => Author::orderBy('id')->get();

        $buildKey = null;
        DB::listen(function () use (&$buildKey): void {
            $buildKey ??= $this->cacheKeysMatching(':build:')[0] ?? null;
        });

        $query();
        $this->assertIsString($buildKey, 'expected a build lease to be held across the database read');

        $this->cacheStore()->delete([
            ...$this->cacheEntryKeys(),
            ...$this->cacheKeysMatching(':r:g'),
        ]);

        $this->cacheStore()->claimBuild($buildKey, str_repeat('f', 32), 5);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $contended = $query();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertCount(1, $contended);
        $this->assertSame('Alice', $contended->first()->name);
        $this->assertNotSame([], $queries, 'a losing claimant must fall through to the database');
        $published = array_values(array_filter(
            $this->cacheQueryKeysWithField('m'),
            static fn(string $key): bool => !str_contains($key, ':build:'),
        ));

        $this->assertSame([], $published, 'a losing claimant must not publish');
    }

    /** A planted query overlay detects retries through the wrong cache route. */
    public function test_a_direct_primary_key_waiter_retries_through_the_row_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $token = 'i:' . $author->id;

        $statement = null;
        DB::listen(function ($query) use (&$statement): void {
            $statement ??= [$query->sql, $query->bindings];
        });

        Author::find($author->id);
        $this->assertIsArray($statement, 'expected the warming read to compile a statement');
        $this->assertNotSame([], $this->cacheKeysMatching(':r:g'), 'expected a canonical row');

        $table = $this->table('authors');
        $keys = $this->cacheKeys();
        $store = $this->cacheStore();
        $queryHash = $this->app->make(QueryIdentity::class)->hash(
            route: QueryPlan::DIRECT_PK,
            rootHash: $table->hash,
            dependencyHashes: [$table->hash],
            sql: $statement[0],
            bindings: DB::connection()->prepareBindings($statement[1]),
            namespace: 'u',
            operation: 'select',
        );

        $store->delete($this->cacheKeysMatching(':r:g'));
        $store->writeHashField(
            $keys->queryEntry($table, 'u', $queryHash),
            'r',
            $this->app->make(RawResultCodec::class)->encode(
                [(object) ['id' => $author->id, 'name' => 'Planted']],
                $this->app->make(CacheRuntime::class)->epoch(),
                $store->getRaw($keys->version($table)) ?? '0',
            ),
        );

        $store->claimBuild($keys->rowBuild($table, '0', $token), str_repeat('f', 32), 30);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $found = Author::find($author->id);
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotNull($found);
        $this->assertSame(
            'Alice',
            $found->name,
            'a direct-pk waiter must not serve the query-entry key its route never writes',
        );
        $this->assertNotSame(
            [],
            $queries,
            'a direct-pk waiter whose row is still missing must fall through to the database',
        );
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

        $started = hrtime(true);
        $woken = $this->cacheStore()->brpop((string) $waiter->wakeKey, 2.0);
        $elapsedMs = (hrtime(true) - $started) / 1e6;

        $this->assertTrue($woken);
        $this->assertLessThan(1000, $elapsedMs);
    }

    public function test_an_empty_lease_token_neither_publishes_nor_releases(): void
    {
        $table = $this->table();
        $keys = $this->cacheKeys();
        $buildKey = $keys->queryBuild($table, '1', 'n', 'hash');
        $entryKey = $keys->queryEntry($table, 'n', 'hash');
        $owner = str_repeat('a', 32);
        $this->cacheStore()->claimBuild($buildKey, $owner, 30);

        $published = $this->cacheStore()->publishVersionedEntries(
            entryKeys: [$entryKey],
            entryPayloads: ['payload'],
            ttl: 30,
            versionKeys: [],
            expectedVersions: [],
            buildingKey: $buildKey,
            wakeKey: null,
            token: null,
            wakeTtl: 10,
        );

        $this->assertFalse($published);
        $this->assertSame(
            $owner,
            $this->cacheStore()->getRaw($buildKey),
            'an empty token owns nothing, so another claimant\'s lease must survive',
        );
        $this->assertNull(
            $this->cacheStore()->getRaw($entryKey),
            'nothing may be published while another claimant holds the lease',
        );
    }

    private function plan(): QueryPlan
    {
        $root = $this->table();

        return QueryPlan::canonical(
            $root,
            [$root],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
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

    private function table(string $name = 'posts'): TableIdentity
    {
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $name);
        $this->assertNotNull($table);

        return $table;
    }
}
