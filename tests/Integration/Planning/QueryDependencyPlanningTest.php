<?php

namespace NormCache\Tests\Integration\Planning;

use Illuminate\Support\Facades\DB;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheStrategy;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;

class QueryDependencyPlanningTest extends TestCase
{
    public function test_where_in_closure_tracks_its_subquery_table_or_bypasses(): void
    {
        $plan = $this->modelsPlan(
            Author::whereIn('id', fn($query) => $query->from('posts')->select('author_id')),
        );

        $this->assertTracksTableOrBypasses($plan, 'testing:posts');
    }

    public function test_or_where_in_query_builder_tracks_its_subquery_table_or_bypasses(): void
    {
        $plan = $this->modelsPlan(
            Author::where('name', 'Alice')->orWhereIn('id', Post::select('author_id')),
        );

        $this->assertTracksTableOrBypasses($plan, 'testing:posts');
    }

    public function test_or_where_not_in_query_builder_tracks_its_subquery_table_or_bypasses(): void
    {
        $plan = $this->modelsPlan(
            Author::where('name', 'Alice')->orWhereNotIn('id', Post::select('author_id')),
        );

        $this->assertTracksTableOrBypasses($plan, 'testing:posts');
    }

    public function test_raw_expression_subquery_predicate_bypasses(): void
    {
        $plan = $this->modelsPlan(
            Author::where(
                DB::raw('(select count(*) from posts where posts.author_id = authors.id)'),
                '>',
                3,
            ),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertNotEmpty($plan->bypassReasons['dependency'] ?? []);
    }

    public function test_raw_expression_in_predicate_bypasses(): void
    {
        $plan = $this->modelsPlan(
            Author::whereIn('id', [DB::raw('select author_id from banned_authors')]),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertNotEmpty($plan->bypassReasons['dependency'] ?? []);
    }

    public function test_nested_where_preserves_captured_subquery_dependencies(): void
    {
        $plan = $this->modelsPlan(
            Author::where(fn($query) => $query->whereIn('id', Post::select('author_id'))),
        );

        $this->assertSame(CacheStrategy::VersionedResult, $plan->strategy);
        $this->assertContains('testing:posts', $plan->dependencies->tables);
    }

    public function test_nested_where_preserves_captured_safety_reasons(): void
    {
        $plan = $this->modelsPlan(
            Author::where(fn($query) => $query->whereHas('lockedPosts')),
        );

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertNotEmpty($plan->bypassReasons['safety'] ?? []);
    }

    public function test_relation_callback_lock_bypasses_and_writes_no_result_cache(): void
    {
        $builder = Author::whereHas('posts', fn($query) => $query->lockForUpdate());

        $this->assertSame(CacheStrategy::LiveQuery, $this->modelsPlan($builder)->strategy);

        $builder->get();

        $this->assertEmpty($this->redisKeys('result:*'));
    }

    public function test_subquery_dependency_uses_the_subquery_connection_namespace(): void
    {
        config()->set('database.connections.secondary_testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
        DB::purge('secondary_testing');

        $plan = $this->modelsPlan(
            Author::query()->whereIn('id', Author::on('secondary_testing')->select('id')),
        );

        $this->assertContains('secondary_testing:authors', $plan->dependencies->tables);
        $this->assertNotContains('testing:authors', $plan->dependencies->tables);
    }

    public function test_scalar_plan_honours_captured_dependency_reasons(): void
    {
        $plan = $this->scalarPlan($this->opaqueSelectSubqueryWithHaving());

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertNotEmpty($plan->bypassReasons['dependency'] ?? []);
    }

    public function test_pagination_count_plan_honours_captured_dependency_reasons(): void
    {
        $plan = $this->paginationPlan($this->opaqueSelectSubqueryWithHaving());

        $this->assertSame(CacheStrategy::LiveQuery, $plan->strategy);
        $this->assertNotEmpty($plan->bypassReasons['dependency'] ?? []);
    }

    private function opaqueSelectSubqueryWithHaving(): CacheableBuilder
    {
        return Author::selectSub(
            fn($query) => $query->from('posts')
                ->selectRaw('count(*)')
                ->whereColumn('posts.author_id', 'authors.id'),
            'x',
        )->having('x', '>', 0);
    }

    private function modelsPlan(CacheableBuilder $builder): CachePlan
    {
        return $this->plan($builder, CachePlanContext::models());
    }

    private function scalarPlan(CacheableBuilder $builder): CachePlan
    {
        return $this->plan($builder, CachePlanContext::scalar(['*']));
    }

    private function paginationPlan(CacheableBuilder $builder): CachePlan
    {
        return $this->plan($builder, CachePlanContext::paginationCount());
    }

    private function plan(CacheableBuilder $builder, CachePlanContext $context): CachePlan
    {
        $prepared = $builder->prepareCacheExecution();

        return $prepared->builder->cachePlan($prepared->base, $context);
    }

    private function assertTracksTableOrBypasses(CachePlan $plan, string $table): void
    {
        $this->assertTrue(
            $plan->strategy === CacheStrategy::LiveQuery || in_array($table, $plan->dependencies->tables, true),
            "Expected the plan to bypass or track [{$table}], got [{$plan->strategy->name}] with dependencies ["
                . implode(', ', $plan->dependencies->tables) . '].',
        );
    }
}
