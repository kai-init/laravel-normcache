<?php

namespace NormCache\Tests\Unit\Cache;

use Illuminate\Database\Eloquent\Collection;
use NormCache\Cache\ExecutionEngine;
use NormCache\Enums\CacheStatus;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ResultCacheResult;
use PHPUnit\Framework\TestCase;

class ExecutionEngineTest extends TestCase
{
    private ExecutionEngine $executor;

    protected function setUp(): void
    {
        parent::setUp();
        $this->executor = new ExecutionEngine;
    }

    // -------------------------------------------------------------------------
    // runResult
    // -------------------------------------------------------------------------

    public function test_run_result_calls_on_hit_and_returns_collection(): void
    {
        $hit = new ResultCacheResult(CacheStatus::Hit, 'k', ['data'], null, null, null, [], []);
        $hitCalled = false;

        $result = $this->executor->runResult(
            fetch: fn() => $hit,
            waitForBuild: fn() => null,
            onMiss: fn($r) => [new Collection, []],
            onStore: fn($p, $r) => null,
            onHit: function ($r) use (&$hitCalled) {
                $hitCalled = true;

                return new Collection;
            },
            onBuild: fn() => new Collection,
        );

        $this->assertTrue($hitCalled);
        $this->assertInstanceOf(Collection::class, $result);
    }

    public function test_run_result_calls_on_miss_store_and_returns_models(): void
    {
        $miss = new ResultCacheResult(CacheStatus::Miss, 'k', null, null, null, null, [], []);
        $storeCalled = false;
        $models = new Collection;

        $result = $this->executor->runResult(
            fetch: fn() => $miss,
            waitForBuild: fn() => null,
            onMiss: fn($r) => [$models, ['payload']],
            onStore: function ($p, $r) use (&$storeCalled) {
                $storeCalled = true;
            },
            onHit: fn($r) => new Collection,
            onBuild: fn() => new Collection,
        );

        $this->assertTrue($storeCalled);
        $this->assertSame($models, $result);
    }

    public function test_run_result_calls_on_build_when_wait_returns_null(): void
    {
        $building = new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        $buildCalled = false;

        $this->executor->runResult(
            fetch: fn() => $building,
            waitForBuild: fn() => null,
            onMiss: fn($r) => [new Collection, []],
            onStore: fn($p, $r) => null,
            onHit: fn($r) => new Collection,
            onBuild: function () use (&$buildCalled) {
                $buildCalled = true;

                return new Collection;
            },
        );

        $this->assertTrue($buildCalled);
    }

    public function test_run_result_retries_hit_after_successful_wait(): void
    {
        $building = new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        $hit = new ResultCacheResult(CacheStatus::Hit, 'k', ['data'], null, null, null, [], []);
        $hitCalled = false;

        $this->executor->runResult(
            fetch: fn() => $building,
            waitForBuild: fn() => $hit,
            onMiss: fn($r) => [new Collection, []],
            onStore: fn($p, $r) => null,
            onHit: function ($r) use (&$hitCalled) {
                $hitCalled = true;

                return new Collection;
            },
            onBuild: fn() => new Collection,
        );

        $this->assertTrue($hitCalled);
    }

    // -------------------------------------------------------------------------
    // runPivot
    // -------------------------------------------------------------------------

    public function test_run_pivot_calls_on_hit_when_no_missed_ids(): void
    {
        $pivotResult = new PivotCacheResult('v1', [1 => [['id' => 5]]], [], []);
        $hitCalled = false;

        $this->executor->runPivot(
            fetch: fn() => $pivotResult,
            waitForBuild: fn() => null,
            onBuild: fn() => new Collection,
            onMiss: fn() => new Collection,
            onStore: fn($c, $r) => null,
            onHit: function ($r) use (&$hitCalled) {
                $hitCalled = true;

                return new Collection;
            },
        );

        $this->assertTrue($hitCalled);
    }

    public function test_run_pivot_calls_on_miss_and_store_when_missed_ids_present(): void
    {
        $pivotResult = new PivotCacheResult('v1', [1 => null, 2 => [['id' => 5]]], [], []);
        $missCalled = false;
        $storeCalled = false;

        $this->executor->runPivot(
            fetch: fn() => $pivotResult,
            waitForBuild: fn() => null,
            onBuild: fn() => new Collection,
            onMiss: function () use (&$missCalled) {
                $missCalled = true;

                return new Collection;
            },
            onStore: function ($c, $r) use (&$storeCalled) {
                $storeCalled = true;
            },
            onHit: fn($r) => new Collection,
        );

        $this->assertTrue($missCalled);
        $this->assertTrue($storeCalled);
    }

    public function test_run_pivot_can_store_raw_models_and_return_transformed_models(): void
    {
        $pivotResult = new PivotCacheResult('v1', [1 => null], [], []);
        $raw = new Collection(['raw']);
        $visible = new Collection(['visible']);
        $stored = null;

        $result = $this->executor->runPivot(
            fetch: fn() => $pivotResult,
            waitForBuild: fn() => null,
            onBuild: fn() => new Collection,
            onMiss: fn() => [$visible, $raw],
            onStore: function ($models) use (&$stored) {
                $stored = $models;
            },
            onHit: fn() => new Collection,
        );

        $this->assertSame($visible, $result);
        $this->assertSame($raw, $stored);
    }

    // -------------------------------------------------------------------------
    // runNormalized
    // -------------------------------------------------------------------------

    public function test_run_normalized_calls_on_hit_with_result(): void
    {
        $hit = new QueryCacheResult(CacheStatus::Hit, 'k', [1, 2], null, null, null, [], []);
        $received = null;

        $this->executor->runNormalized(
            fetch: fn() => $hit,
            waitForBuild: fn() => null,
            onHit: function ($r) use (&$received) {
                $received = $r;

                return new Collection;
            },
            onMiss: fn($r) => new Collection,
            onBuild: fn() => new Collection,
        );

        $this->assertSame($hit, $received);
    }

    public function test_run_normalized_stale_routes_to_on_hit(): void
    {
        $stale = new QueryCacheResult(CacheStatus::Stale, null, [3], null, null, null, [], []);
        $hitCalled = false;

        $this->executor->runNormalized(
            fetch: fn() => $stale,
            waitForBuild: fn() => null,
            onHit: function ($r) use (&$hitCalled) {
                $hitCalled = true;

                return new Collection;
            },
            onMiss: fn($r) => new Collection,
            onBuild: fn() => new Collection,
        );

        $this->assertTrue($hitCalled);
    }

    public function test_run_normalized_calls_on_miss_with_result(): void
    {
        $miss = new QueryCacheResult(CacheStatus::Miss, 'k', null, null, 'bk', 'tok', ['ver:k:'], ['5']);
        $received = null;

        $this->executor->runNormalized(
            fetch: fn() => $miss,
            waitForBuild: fn() => null,
            onHit: fn($r) => new Collection,
            onMiss: function ($r) use (&$received) {
                $received = $r;

                return new Collection;
            },
            onBuild: fn() => new Collection,
        );

        $this->assertSame($miss, $received);
    }

    public function test_run_normalized_calls_on_build_when_wait_returns_null(): void
    {
        $building = new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        $buildCalled = false;

        $this->executor->runNormalized(
            fetch: fn() => $building,
            waitForBuild: fn() => null,
            onHit: fn($r) => new Collection,
            onMiss: fn($r) => new Collection,
            onBuild: function () use (&$buildCalled) {
                $buildCalled = true;

                return new Collection;
            },
        );

        $this->assertTrue($buildCalled);
    }

    public function test_run_normalized_retries_after_successful_wait(): void
    {
        $building = new QueryCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        $hit = new QueryCacheResult(CacheStatus::Hit, 'k', [1], null, null, null, [], []);
        $hitCalled = false;

        $this->executor->runNormalized(
            fetch: fn() => $building,
            waitForBuild: fn() => $hit,
            onHit: function ($r) use (&$hitCalled) {
                $hitCalled = true;

                return new Collection;
            },
            onMiss: fn($r) => new Collection,
            onBuild: fn() => new Collection,
        );

        $this->assertTrue($hitCalled);
    }

    // -------------------------------------------------------------------------
    // runScalar
    // -------------------------------------------------------------------------

    public function test_run_scalar_calls_on_hit_on_cache_hit(): void
    {
        $hit = new ResultCacheResult(CacheStatus::Hit, 'k', 42, null, null, null, [], []);
        $received = null;

        $this->executor->runScalar(
            fetch: fn() => $hit,
            waitForBuild: fn() => null,
            compute: fn() => 0,
            onStore: fn($v, $r) => null,
            onHit: function ($r) use (&$received) {
                $received = $r;

                return $r->payload;
            },
        );

        $this->assertSame($hit, $received);
    }

    public function test_run_scalar_calls_compute_and_store_on_miss(): void
    {
        $miss = new ResultCacheResult(CacheStatus::Miss, 'k', null, null, null, null, [], []);
        $storeCalled = false;

        $result = $this->executor->runScalar(
            fetch: fn() => $miss,
            waitForBuild: fn() => null,
            compute: fn() => 99,
            onStore: function ($v, $r) use (&$storeCalled) {
                $storeCalled = true;
            },
            onHit: fn($r) => $r->payload,
        );

        $this->assertTrue($storeCalled);
        $this->assertSame(99, $result);
    }

    public function test_run_scalar_calls_compute_on_build_budget_exhausted(): void
    {
        $building = new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);

        $result = $this->executor->runScalar(
            fetch: fn() => $building,
            waitForBuild: fn() => null,
            compute: fn() => 7,
            onStore: fn($v, $r) => null,
            onHit: fn($r) => $r->payload,
        );

        $this->assertSame(7, $result);
    }

    public function test_run_scalar_retries_after_successful_wait(): void
    {
        $building = new ResultCacheResult(CacheStatus::Building, null, null, null, null, null, [], []);
        $hit = new ResultCacheResult(CacheStatus::Hit, 'k', 55, null, null, null, [], []);
        $hitCalled = false;

        $this->executor->runScalar(
            fetch: fn() => $building,
            waitForBuild: fn() => $hit,
            compute: fn() => 0,
            onStore: fn($v, $r) => null,
            onHit: function ($r) use (&$hitCalled) {
                $hitCalled = true;

                return $r->payload;
            },
        );

        $this->assertTrue($hitCalled);
    }
}
