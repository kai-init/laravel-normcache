<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\Log;
use NormCache\Planning\CachePlanner;
use NormCache\Tests\TestCase;
use NormCache\Values\DependencySet;
use NormCache\Values\QueryAnalysis;

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

        $analysis = new QueryAnalysis(['*'], [], [], ['posts', 'authors']);

        $dependencies = new DependencySet([], ['posts']);

        $reflection = new \ReflectionMethod($planner, 'checkDependencyCompleteness');
        $reflection->setAccessible(true);
        $reflection->invoke($planner, $analysis, $dependencies, 'posts');
    }
}
