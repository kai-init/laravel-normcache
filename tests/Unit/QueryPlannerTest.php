<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\QueryPlanner;
use NormCache\Tests\Fixtures\Models\Post;
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
        $this->posts = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'posts');
    }

    public function test_root_wildcard_uses_canonical_rows(): void
    {
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
        $query = DB::query()->from('posts');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            $primaryKey,
            [$this->posts],
        );

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
        $this->assertSame($primaryKey, $plan->primaryKey);
        $this->assertFalse($plan->materializeResult);
    }

    public function test_bare_alias_wildcard_uses_canonical_rows(): void
    {
        $query = DB::query()->from('posts p')->select('p.*');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
    }

    public function test_dependency_backed_view_uses_vector_validated_result_storage(): void
    {
        $view = TableIdentity::fromParts(
            'sqlite',
            'testing',
            '/tmp/test.sqlite',
            '',
            '',
            'post_titles',
            true,
        );
        $query = DB::query()->from('post_titles')->select('*');

        $plan = $this->planner->plan(
            $query,
            $view,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$view, $this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
    }

    public function test_result_cache_override_materializes_wildcard_canonical_result(): void
    {
        $query = DB::query()->from('posts')->useResultCache();

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
        $this->assertTrue($plan->materializeResult);
    }

    public function test_result_cache_override_keeps_primary_key_query_on_direct_row_route(): void
    {
        $query = DB::query()
            ->from('posts')
            ->where('id', 42)
            ->limit(1)
            ->useResultCache();

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::DIRECT_PK, $plan->route);
        $this->assertSame('i:42', $plan->primaryKeyToken);
        $this->assertFalse($plan->materializeResult);
    }

    public function test_narrow_projection_uses_result_route(): void
    {
        $query = DB::query()->from('posts')->select(['id', 'title as heading']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
    }

    public function test_scalar_primary_key_wildcard_uses_direct_row(): void
    {
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
        $query = DB::query()->from('posts')->where('id', 42)->limit(1);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            $primaryKey,
            [$this->posts],
        );

        $this->assertSame(QueryPlan::DIRECT_PK, $plan->route);
        $this->assertSame($primaryKey, $plan->primaryKey);
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

        $this->assertSame(QueryPlan::RESULT, $plan->route);
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

        $this->assertSame(QueryPlan::RESULT, $plan->route);
    }

    public function test_narrow_primary_key_projection_with_bare_columns_is_eligible_for_row_fallback(): void
    {
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
        $query = DB::query()->from('posts')->where('id', 42)->select(['id', 'title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            $primaryKey,
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertSame($primaryKey, $plan->primaryKey);
        $this->assertSame('i:42', $plan->primaryKeyToken);
        $this->assertSame(['id', 'title'], $plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_with_root_qualified_columns_is_eligible(): void
    {
        $query = DB::query()->from('posts')->where('posts.id', 42)->select(['posts.id', 'posts.title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame('i:42', $plan->primaryKeyToken);
        $this->assertSame(['id', 'title'], $plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_with_aliased_from_and_qualified_columns_is_eligible(): void
    {
        $query = DB::query()->from('posts as p')->where('p.id', 42)->select(['p.id', 'p.title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame('i:42', $plan->primaryKeyToken);
        $this->assertSame(['id', 'title'], $plan->projectedColumns);
    }

    public function test_aliased_from_rejects_projection_qualified_by_original_table(): void
    {
        $query = DB::query()->from('posts as p')->where('p.id', 42)->select(['posts.title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertNull($plan->projectedColumns);
    }

    public function test_aliased_from_rejects_wildcard_qualified_by_original_table(): void
    {
        $query = DB::query()->from('posts as p')->where('p.id', 42)->select(['posts.*']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertNull($plan->primaryKeyToken);
    }

    public function test_primary_key_predicate_with_unrelated_qualifier_rejects_direct_row_but_keeps_membership_fallback(): void
    {
        $query = DB::query()->from('posts')->where('authors.id', 42)->select(['title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['title'], $plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_with_column_alias_keeps_result_route_without_token(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->select(['id', 'title as heading']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertNull($plan->primaryKeyToken);
        $this->assertNull($plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_with_raw_expression_keeps_result_route_without_token(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->select(['id', DB::raw('UPPER(title)')]);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertNull($plan->primaryKeyToken);
        $this->assertNull($plan->projectedColumns);
    }

    public function test_extra_predicate_rejects_direct_row_but_keeps_membership_fallback(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->where('published', true)->select(['title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['title'], $plan->projectedColumns);
    }

    public function test_incompatible_direct_limit_keeps_membership_fallback(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->limit(5)->select(['title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['title'], $plan->projectedColumns);
    }

    public function test_multi_row_plain_projection_is_eligible_for_canonical_membership_fallback(): void
    {
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
        $query = DB::query()
            ->from('posts')
            ->where('published', true)
            ->orderBy('id')
            ->select(['id', 'title']);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            $primaryKey,
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertSame($primaryKey, $plan->primaryKey);
        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['id', 'title'], $plan->projectedColumns);
    }

    public function test_aliased_or_raw_projection_is_not_eligible_for_canonical_membership_fallback(): void
    {
        $query = DB::query()
            ->from('posts')
            ->where('published', true)
            ->selectRaw('id, upper(title) as heading');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertNull($plan->projectedColumns);
    }

    public function test_group_limited_queries_do_not_publish_canonical_rows(): void
    {
        $query = DB::query()->from('posts');
        $query->groupLimit = ['value' => 1, 'column' => 'author_id'];

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertNull($plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_with_tag_override_uses_membership_not_direct_row_fallback(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->select(['title'])->tag('reports');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['title'], $plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_with_ttl_override_uses_membership_not_direct_row_fallback(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->select(['title'])->ttl(60);

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['title'], $plan->projectedColumns);
    }

    public function test_narrow_primary_key_projection_computes_soft_delete_mode(): void
    {
        $query = DB::query()->from('posts')->where('id', 42)->select(['title'])
            ->markCacheableModel(Post::class, 'id', 'int', 'deleted_at');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame('i:42', $plan->primaryKeyToken);
        $this->assertSame('with', $plan->softDeleteMode);
        $this->assertSame('deleted_at', $plan->deletedAtColumn);
    }

    public function test_multiple_soft_delete_predicates_disable_direct_row_but_keep_membership_fallback(): void
    {
        $query = DB::query()->from('posts')
            ->where('id', 42)
            ->whereNull('posts.deleted_at')
            ->whereNotNull('posts.deleted_at')
            ->select(['title'])
            ->markCacheableModel(Post::class, 'id', 'int', 'deleted_at');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertNull($plan->primaryKeyToken);
        $this->assertSame(['title'], $plan->projectedColumns);
    }

    public function test_unsafe_soft_delete_direct_candidate_still_uses_canonical_membership(): void
    {
        $query = DB::query()->from('posts')
            ->where('id', 42)
            ->whereNull('posts.deleted_at')
            ->whereNotNull('posts.deleted_at')
            ->markCacheableModel(Post::class, 'id', 'int', 'deleted_at');

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts],
        );

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
        $this->assertNull($plan->primaryKeyToken);
    }

    public function test_result_cache_override_does_not_collapse_query_group(): void
    {
        $comments = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'comments');
        $query = DB::query()
            ->from('posts')
            ->join('comments', 'comments.post_id', '=', 'posts.id')
            ->useResultCache();

        $plan = $this->planner->plan(
            $query,
            $this->posts,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            [$this->posts, $comments],
        );

        $this->assertSame(QueryPlan::QUERY_GROUP, $plan->route);
    }

    public function test_any_sql_join_uses_query_group(): void
    {
        $comments = TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'comments');
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
