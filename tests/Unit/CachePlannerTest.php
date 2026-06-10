<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\Log;
use NormCache\Enums\CacheMode;
use NormCache\Enums\CacheStrategy;
use NormCache\Planning\CachePlanner;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;

class CachePlannerTest extends TestCase
{
    public function test_planner_logs_warning_for_under_declared_dependencies_in_debug_mode(): void
    {
        config(['app.debug' => true]);

        Log::shouldReceive('warning')
            ->once()
            ->withArgs(function ($message) {
                return str_contains($message, 'under-declared dependency') && str_contains($message, 'authors');
            });

        $planner = new CachePlanner;

        $dependencies = new DependencySet([], ['posts']);

        $reflection = new \ReflectionMethod($planner, 'checkDependencyCompleteness');
        $reflection->setAccessible(true);
        $reflection->invoke($planner, ['posts', 'authors'], $dependencies, 'posts');
    }

    public function test_successful_hot_plan_does_not_build_reason_strings(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertSame(CacheMode::Normalized, $plan->mode);
        $this->assertSame([], $plan->reasons);
        $this->assertSame([], $plan->bypassReasons);
    }

    public function test_bypass_plan_still_contains_human_readable_reasons(): void
    {
        $prepared = Author::orderByRaw('CASE WHEN id = ? THEN 0 ELSE 1 END', [1])
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertContains('raw ORDER expression', $plan->bypassReasons['dependency']);
    }

    public function test_global_opted_out_bypass_precedes_query_inspection(): void
    {
        $prepared = Author::withoutCache()
            ->orderByRaw('CASE WHEN id = ? THEN 0 ELSE 1 END', [1])
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertSame(
            ['opted_out' => ['withoutCache() was called explicitly']],
            $plan->bypassReasons,
        );
    }

    public function test_simple_scalar_query_uses_versioned_result_strategy(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar('count', ['*']),
        );

        $this->assertSame(CacheMode::Result, $plan->mode);
        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertSame([Author::class], $plan->dependencies->models);
    }

    public function test_scalar_query_with_raw_dependency_clause_bypasses(): void
    {
        $prepared = Author::whereRaw('name = ?', ['Alice'])->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar('count', ['*']),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertContains('raw WHERE expression', $plan->bypassReasons['dependency']);
    }

    public function test_scalar_query_with_raw_order_bypasses(): void
    {
        $prepared = Author::orderByRaw('CASE WHEN id = ? THEN 0 ELSE 1 END', [1])
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar('value', ['name']),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertContains('raw ORDER expression', $plan->bypassReasons['dependency']);
    }

    public function test_grouped_scalar_query_preserves_result_cache_behavior(): void
    {
        $prepared = Author::groupBy('name')
            ->having('name', '!=', '')
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar('count', ['name']),
        );

        $this->assertSame(CacheMode::Result, $plan->mode);
        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertSame(['name'], $plan->columns);
    }

    public function test_scalar_context_reason_skips_fast_path(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar(
                'count',
                ['*'],
                contextReasons: ['opted_out' => ['test bypass']],
            ),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertSame(['test bypass'], $plan->bypassReasons['opted_out']);
    }

    public function test_scalar_join_without_dependencies_bypasses(): void
    {
        $prepared = Author::join('posts', 'posts.author_id', '=', 'authors.id')
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar('count', ['*']),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertContains('complex_query_requires_depends_on', $plan->bypassReasons['dependency']);
    }

    public function test_locked_scalar_query_bypasses(): void
    {
        $prepared = Author::lockForUpdate()->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar('count', ['*']),
        );

        $this->assertSame(CacheMode::Bypass, $plan->mode);
        $this->assertContains('query lock (SELECT FOR UPDATE)', $plan->bypassReasons['safety']);
    }
}
