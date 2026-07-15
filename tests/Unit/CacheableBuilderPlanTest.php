<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\ProjectionClassifier;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlanContext;

class CacheableBuilderPlanTest extends UnitTestCase
{
    public function test_plan_prepared_matches_direct_plan(): void
    {
        $builder = Author::query()->where('name', 'A');
        $prepared = $builder->prepareCacheExecution();
        $context = fn() => CachePlanContext::models(
            ProjectionClassifier::resolve($prepared->base, ['*']),
            selectAll: true,
        );

        $this->assertEquals(
            $builder->cachePlan($prepared->base, $context()),
            $builder->planPrepared($prepared, $context),
        );
    }

    public function test_plan_prepared_infers_join_table_dependency(): void
    {
        $builder = Post::query()
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->select('posts.*');
        $prepared = $builder->prepareCacheExecution();

        $plan = $builder->planPrepared(
            $prepared,
            fn() => CachePlanContext::models(
                ProjectionClassifier::resolve($prepared->base, ['*']),
                selectAll: false,
            ),
        );

        $this->assertContains('testing:authors', $plan->dependencies->tables);
    }
}
