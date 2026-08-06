<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Database\QueryStatement;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryObserver;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;
use Psr\Log\NullLogger;

final class QueryObserverTest extends UnitTestCase
{
    public function test_query_outcomes_emit_their_full_event_payloads(): void
    {
        Event::fake([
            QueryCacheHit::class,
            QueryCacheMiss::class,
            QueryCacheRepaired::class,
        ]);

        $observer = new QueryObserver(
            CacheConfig::fromArray([...config('normcache'), 'events' => true]),
            null,
            new FailureReporter(new NullLogger),
        );
        $table = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'posts');
        $plan = QueryPlan::queryGroup($table, [$table]);
        $query = DB::table('posts');
        $sql = 'select * from posts where id = ?';
        $bindings = [42];
        $statement = new QueryStatement(fn(): array => [$sql, $bindings]);

        $observer->hit($query, $plan, 'hit-hash', $statement, 'row_cache_fallback');
        $observer->miss($query, $plan, 'miss-hash', $statement);
        $observer->repaired($query, $plan, 'repair-hash', $statement, 'row_repair');

        Event::assertDispatched(
            QueryCacheHit::class,
            fn(QueryCacheHit $event): bool => $event->route === 'query_group'
                && $event->queryHash === 'hit-hash'
                && $event->tableHash === $table->hash
                && $event->sql === $sql
                && $event->bindings === $bindings
                && $event->reason === 'row_cache_fallback',
        );
        Event::assertDispatched(
            QueryCacheMiss::class,
            fn(QueryCacheMiss $event): bool => $event->route === 'query_group'
                && $event->queryHash === 'miss-hash'
                && $event->tableHash === $table->hash
                && $event->sql === $sql
                && $event->bindings === $bindings
                && $event->reason === null,
        );
        Event::assertDispatched(
            QueryCacheRepaired::class,
            fn(QueryCacheRepaired $event): bool => $event->route === 'query_group'
                && $event->queryHash === 'repair-hash'
                && $event->tableHash === $table->hash
                && $event->sql === $sql
                && $event->bindings === $bindings
                && $event->reason === 'row_repair',
        );
    }

    public function test_corrupt_payload_diagnostic_is_reported_at_most_once_per_scope_per_key(): void
    {
        Event::fake([QueryCacheMiss::class]);

        $observer = new QueryObserver(
            CacheConfig::fromArray([...config('normcache'), 'events' => true]),
            null,
            new FailureReporter(new NullLogger),
        );
        $table = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'posts');
        $plan = QueryPlan::result($table, []);
        $query = DB::query()->from('posts');
        $statement = new QueryStatement(static fn(): array => ['select * from posts', []]);

        // Same corrupted key observed twice (e.g. a concurrent request racing the
        // same still-corrupt payload) must only be reported once.
        $observer->miss($query, $plan, 'hash-a', $statement, 'corrupt_payload');
        $observer->miss($query, $plan, 'hash-a', $statement, 'corrupt_payload');

        // A different corrupted key is a distinct occurrence and must still report.
        $observer->miss($query, $plan, 'hash-b', $statement, 'corrupt_payload');

        // Dedup is specific to corrupt_payload; ordinary misses are unaffected.
        $observer->miss($query, $plan, 'hash-c', $statement);
        $observer->miss($query, $plan, 'hash-c', $statement);

        Event::assertDispatchedTimes(QueryCacheMiss::class, 4);
    }
}
