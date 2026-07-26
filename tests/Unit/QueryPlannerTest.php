<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\QueryPlanner;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlannerTest extends UnitTestCase
{
    private QueryPlanner $planner;

    private TableIdentity $posts;

    protected function setUp(): void
    {
        parent::setUp();

        $this->planner = new QueryPlanner;
        $this->posts = TableIdentity::fromParts('sqlite', 'test', 'testing', '/tmp/test.sqlite', '', '', 'posts');
    }

    public function test_root_wildcard_uses_canonical_rows(): void
    {
        $query = DB::query()->from('posts');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
    }

    public function test_narrow_projection_uses_exact_result(): void
    {
        $query = DB::query()->from('posts')->select(['id', 'title as heading']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::EXACT, $plan->route);
    }

    public function test_scalar_primary_key_wildcard_uses_direct_row(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->limit(1);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::DIRECT_PK, $plan->route);
        $this->assertSame('i:42', $plan->primaryKeyToken);
    }

    public function test_exists_never_uses_a_canonical_row_route(): void
    {
        $query = DB::query()->from('posts')->where('id', 42);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
            operation: 'exists',
        );

        $this->assertSame(QueryPlan::EXACT, $plan->route);
    }

    public function test_primary_key_aggregate_never_uses_a_canonical_row_route(): void
    {
        $query = DB::query()->from('posts')->where('id', 42);
        $query->aggregate = ['function' => 'count', 'columns' => ['*']];

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::EXACT, $plan->route);
    }

    public function test_any_sql_join_uses_query_group(): void
    {
        $comments = TableIdentity::fromParts('sqlite', 'test', 'testing', '/tmp/test.sqlite', '', '', 'comments');
        $query = DB::query()
            ->from('posts')
            ->join('comments', 'comments.post_id', '=', 'posts.id')
            ->select('posts.*');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts, $comments],
        );

        $this->assertSame(QueryPlan::QUERY_GROUP, $plan->route);
    }
}
