<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\Log;
use NormCache\Enums\CacheStrategy;
use NormCache\Planning\CachePlanner;
use NormCache\Planning\DependencyResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;

class CachePlannerTest extends UnitTestCase
{
    public function test_planner_logs_warning_for_under_declared_dependencies_in_debug_mode(): void
    {
        config(['app.debug' => true]);

        Log::shouldReceive('warning')
            ->once()
            ->withArgs(function ($message) {
                return str_contains($message, 'under-declared dependency') && str_contains($message, 'authors');
            });

        $resolver = new DependencyResolver;

        $dependencies = new DependencySet([], ['posts']);

        $reflection = new \ReflectionMethod($resolver, 'checkDependencyCompleteness');
        $reflection->setAccessible(true);
        $reflection->invoke($resolver, ['posts', 'authors'], $dependencies, 'posts');
    }

    public function test_successful_hot_plan_does_not_build_reason_strings(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertTrue($plan->isNormalized());
        $this->assertSame([], $plan->flatReasons());
        $this->assertSame([], $plan->bypassReasons);
    }

    public function test_active_soft_delete_scope_allows_a_direct_primary_key_plan(): void
    {
        $prepared = Post::whereKey([3, 1, 2])->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertSame(CacheStrategy::DirectModels, $plan->strategy);
        $this->assertSame([1, 2, 3], $plan->primaryKeys);
    }

    public function test_removed_soft_delete_scope_does_not_ignore_a_manual_null_constraint(): void
    {
        $prepared = Post::withTrashed()
            ->whereNull('posts.deleted_at')
            ->whereKey([3, 1, 2])
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertSame(CacheStrategy::NormalizedQuery, $plan->strategy);
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

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
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

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
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
            CachePlanContext::scalar(['*']),
        );

        $this->assertTrue($plan->usesResultCache());
        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertSame([Author::class], $plan->dependencies->models);
    }

    public function test_scalar_query_with_raw_dependency_clause_bypasses(): void
    {
        $prepared = Author::whereRaw('name = ?', ['Alice'])->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar(['*']),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertContains('raw WHERE expression', $plan->bypassReasons['dependency']);
    }

    public function test_scalar_query_with_raw_order_bypasses(): void
    {
        $prepared = Author::orderByRaw('CASE WHEN id = ? THEN 0 ELSE 1 END', [1])
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar(['name']),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
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
            CachePlanContext::scalar(['name']),
        );

        $this->assertTrue($plan->usesResultCache());
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
                ['*'],
                contextReasons: ['opted_out' => ['test bypass']],
            ),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertSame(['test bypass'], $plan->bypassReasons['opted_out']);
    }

    public function test_scalar_join_without_dependencies_bypasses(): void
    {
        $prepared = Author::join('posts', 'posts.author_id', '=', 'authors.id')
            ->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar(['*']),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertContains('complex_query_requires_depends_on', $plan->bypassReasons['dependency']);
    }

    public function test_locked_scalar_query_bypasses(): void
    {
        $prepared = Author::lockForUpdate()->prepareCacheExecution();

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar(['*']),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertContains('query lock (SELECT FOR UPDATE)', $plan->bypassReasons['safety']);
    }

    public function test_exists_where_with_safe_inferred_dependency_is_cached_as_result(): void
    {
        $prepared = Author::query()->prepareCacheExecution();
        $prepared->base->wheres[] = [
            'type' => 'Exists',
            'query' => Author::query()->getQuery(),
            'boolean' => 'and',
        ];

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(inferred: new DependencySet(models: [Post::class])),
        );

        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertTrue($plan->dependencies->safe);
        $this->assertSame([Author::class, Post::class], $plan->dependencies->models);
    }

    public function test_exists_where_without_inferred_dependency_still_bypasses(): void
    {
        $prepared = Author::query()->prepareCacheExecution();
        $prepared->base->wheres[] = [
            'type' => 'Exists',
            'query' => Author::query()->getQuery(),
            'boolean' => 'and',
        ];

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
    }

    public function test_exists_where_combined_with_raw_where_bypasses_even_with_inferred_dependency(): void
    {
        $prepared = Author::query()->prepareCacheExecution();
        $prepared->base->wheres[] = [
            'type' => 'Exists',
            'query' => Author::query()->getQuery(),
            'boolean' => 'and',
        ];
        $prepared->base->wheres[] = [
            'type' => 'raw',
            'sql' => 'LOWER(name) = LOWER(name)',
            'boolean' => 'and',
        ];

        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::models(inferred: new DependencySet(models: [Post::class])),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
    }
}
