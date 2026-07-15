<?php

namespace NormCache\Tests\Unit;

use NormCache\Enums\CacheStrategy;
use NormCache\Planning\CachePlanner;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlanContext;

class CachePlannerTest extends UnitTestCase
{
    public function test_successful_hot_plan_does_not_build_reason_strings(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertTrue($plan->isNormalized());
        $this->assertSame([], $plan->bypassReasons);
    }

    public function test_active_soft_delete_scope_allows_a_direct_primary_key_plan(): void
    {
        $prepared = Post::whereKey([3, 1, 2])->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertSame(CacheStrategy::DirectModels, $plan->strategy);
        $this->assertSame([1, 2, 3], $plan->primaryKeys);
    }

    public function test_removed_soft_delete_scope_does_not_ignore_a_manual_null_constraint(): void
    {
        $prepared = Post::withTrashed()->whereNull('posts.deleted_at')->whereKey([3, 1, 2])->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertSame(CacheStrategy::NormalizedQuery, $plan->strategy);
    }

    public function test_raw_order_bypasses_with_human_readable_reason(): void
    {
        $prepared = Author::orderByRaw('CASE WHEN id = ? THEN 0 ELSE 1 END', [1])->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertContains('raw ORDER expression', $plan->bypassReasons['dependency']);
    }

    public function test_global_opt_out_precedes_query_analysis(): void
    {
        $prepared = Author::withoutCache()->orderByRaw('id')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertSame(['opted_out' => ['withoutCache() was called explicitly']], $plan->bypassReasons);
    }

    public function test_simple_scalar_uses_versioned_result_strategy(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::scalar(['*']));

        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertSame([Author::class], $plan->dependencies->models);
    }

    public function test_raw_scalar_dependency_clause_bypasses(): void
    {
        $prepared = Author::whereRaw('name = ?', ['Alice'])->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::scalar(['*']));

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
    }

    public function test_scalar_context_dependency_reason_is_merged_into_the_inspection(): void
    {
        $prepared = Author::where('name', 'Alice')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan(
            $prepared->builder,
            $prepared->base,
            CachePlanContext::scalar(['*'], ['dependency' => ['custom subquery could not be inferred']]),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertSame(['custom subquery could not be inferred'], $plan->bypassReasons['dependency']);
    }

    public function test_grouped_scalar_preserves_result_cache_behavior(): void
    {
        $prepared = Author::groupBy('name')->having('name', '!=', '')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::scalar(['name']));

        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
    }

    public function test_scalar_join_uses_inferred_table_dependency(): void
    {
        $prepared = Author::join('posts', 'posts.author_id', '=', 'authors.id')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::scalar(['*']));

        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertContains('testing:posts', $plan->dependencies->tables);
    }

    public function test_locked_scalar_query_bypasses(): void
    {
        $prepared = Author::lockForUpdate()->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::scalar(['*']));

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
    }

    public function test_exists_query_uses_query_derived_dependency(): void
    {
        $prepared = Author::whereHas('posts')->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertContains('testing:posts', $plan->dependencies->tables);
    }

    public function test_exists_with_nested_raw_where_bypasses(): void
    {
        $prepared = Author::whereHas('posts', fn($query) => $query->whereRaw('views > 0'))->prepareCacheExecution();
        $plan = (new CachePlanner)->plan($prepared->builder, $prepared->base, CachePlanContext::models());

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertContains('raw WHERE expression', $plan->bypassReasons['dependency']);
    }
}
