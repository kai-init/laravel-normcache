<?php

namespace NormCache\Tests\Unit;

use DebugBar\DataCollector\TimeDataCollector;
use Illuminate\Support\Facades\Event;
use NormCache\Database\QueryStatement;
use NormCache\Debug\DebugBarCollector;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryObserver;
use NormCache\Tests\Fixtures\Models\Post;
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
        $query = Post::query()->toBase();
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
        $query = Post::query()->toBase();
        $statement = new QueryStatement(static fn(): array => ['select * from posts', []]);

        $observer->miss($query, $plan, 'hash-a', $statement, 'corrupt_payload');
        $observer->miss($query, $plan, 'hash-a', $statement, 'corrupt_payload');

        $observer->miss($query, $plan, 'hash-b', $statement, 'corrupt_payload');

        $observer->miss($query, $plan, 'hash-c', $statement);
        $observer->miss($query, $plan, 'hash-c', $statement);

        Event::assertDispatchedTimes(QueryCacheMiss::class, 4);
    }

    public function test_batched_invalidation_reports_the_batch_span_for_every_table(): void
    {
        $collector = $this->collector();
        $observer = $this->collectingObserver($collector);

        $observer->begin();
        usleep(2_000);
        $observer->invalidatedMany([
            ['table' => $this->table('posts'), 'mode' => 'version', 'tokens' => []],
            ['table' => $this->table('authors'), 'mode' => 'version', 'tokens' => []],
            ['table' => $this->table('comments'), 'mode' => 'version', 'tokens' => []],
        ]);

        $durations = $this->durations($collector);

        $this->assertCount(3, $durations);

        foreach ($durations as $duration) {
            $this->assertGreaterThan(0.0, $duration);
            $this->assertSame($durations[0], $duration);
        }
    }

    public function test_a_nested_observation_does_not_consume_the_enclosing_span(): void
    {
        $collector = $this->collector();
        $observer = $this->collectingObserver($collector);

        $observer->begin();
        usleep(4_000);

        // A repair issuing its own bypassed query opens and closes a span inside the
        // read that encloses it.
        $observer->begin();
        $observer->invalidated($this->table('posts'), 'version', []);

        $observer->invalidated($this->table('authors'), 'version', []);

        [$nested, $enclosing] = $this->durations($collector);

        $this->assertGreaterThan(0.004, $enclosing);
        $this->assertGreaterThan($nested, $enclosing);
    }

    private function collector(): DebugBarCollector
    {
        if (!class_exists(TimeDataCollector::class)) {
            $this->markTestSkipped('Timing records need the optional Debugbar collector.');
        }

        return new DebugBarCollector;
    }

    private function collectingObserver(DebugBarCollector $collector): QueryObserver
    {
        return new QueryObserver(
            CacheConfig::fromArray([...config('normcache'), 'events' => false]),
            $collector,
            new FailureReporter(new NullLogger),
        );
    }

    private function table(string $name): TableIdentity
    {
        return TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', $name);
    }

    /** @return list<float> */
    private function durations(DebugBarCollector $collector): array
    {
        return array_map(
            static fn(array $measure): float => (float) $measure['duration'],
            array_values($collector->collect()['measures']),
        );
    }
}
