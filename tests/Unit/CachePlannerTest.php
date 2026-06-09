<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\Log;
use NormCache\Enums\CacheMode;
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
}
