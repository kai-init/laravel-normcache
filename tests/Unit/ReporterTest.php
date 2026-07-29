<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Support\Reporter;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class ReporterTest extends UnitTestCase
{
    public function test_query_outcomes_emit_their_full_event_payloads(): void
    {
        Event::fake([
            QueryCacheHit::class,
            QueryCacheMiss::class,
            QueryCacheRepaired::class,
        ]);

        $reporter = new Reporter(
            CacheConfig::fromArray([...config('normcache'), 'events' => true]),
            null,
        );
        $table = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'posts');
        $plan = new QueryPlan(QueryPlan::QUERY_GROUP, $table, [$table]);
        $query = DB::table('posts');
        $sql = 'select * from posts where id = ?';
        $bindings = [42];

        $reporter->hit($query, $plan, 'hit-hash', $sql, $bindings, 'row_cache_fallback');
        $reporter->miss($query, $plan, 'miss-hash', $sql, $bindings);
        $reporter->repaired($query, $plan, 'repair-hash', $sql, $bindings, 'row_repair');

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

        $reporter = new Reporter(
            CacheConfig::fromArray([...config('normcache'), 'events' => true]),
            null,
        );
        $table = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'posts');
        $plan = new QueryPlan(QueryPlan::RESULT, $table, []);
        $query = DB::query()->from('posts');

        // Same corrupted key observed twice (e.g. a concurrent request racing the
        // same still-corrupt payload) must only be reported once.
        $reporter->miss($query, $plan, 'hash-a', 'select * from posts', [], 'corrupt_payload');
        $reporter->miss($query, $plan, 'hash-a', 'select * from posts', [], 'corrupt_payload');

        // A different corrupted key is a distinct occurrence and must still report.
        $reporter->miss($query, $plan, 'hash-b', 'select * from posts', [], 'corrupt_payload');

        // Dedup is specific to corrupt_payload; ordinary misses are unaffected.
        $reporter->miss($query, $plan, 'hash-c', 'select * from posts', [], null);
        $reporter->miss($query, $plan, 'hash-c', 'select * from posts', [], null);

        Event::assertDispatchedTimes(QueryCacheMiss::class, 4);
    }
}
