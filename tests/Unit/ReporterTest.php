<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheMiss;
use NormCache\Support\Reporter;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\QueryPlan;
use NormCache\Values\RuntimeState;
use NormCache\Values\TableIdentity;

final class ReporterTest extends UnitTestCase
{
    public function test_corrupt_payload_diagnostic_is_reported_at_most_once_per_scope_per_key(): void
    {
        Event::fake([QueryCacheMiss::class]);

        $reporter = new Reporter(
            CacheConfig::fromArray([...config('normcache'), 'events' => true]),
            null,
            new RuntimeState,
        );
        $table = TableIdentity::fromParts('sqlite', 'test', 'testing', '/tmp/test.sqlite', '', '', 'posts');
        $plan = new QueryPlan(QueryPlan::EXACT, $table, []);
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
