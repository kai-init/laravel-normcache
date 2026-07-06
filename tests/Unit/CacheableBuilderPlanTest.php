<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\ProjectionClassifier;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;

class CacheableBuilderPlanTest extends UnitTestCase
{
    public function test_plan_prepared_matches_direct_plan(): void
    {
        $builder = Author::query()->where('name', 'A');
        $prepared = $builder->prepareCacheExecution();

        $viaSeam = $builder->planPrepared($prepared, fn(DependencySet $inferred) => CachePlanContext::models(
            ProjectionClassifier::resolve($prepared->base, ['*']),
            $inferred,
            selectAll: true,
        ));

        $direct = $builder->cachePlan($prepared->base, CachePlanContext::models(
            ProjectionClassifier::resolve($prepared->base, ['*']),
            $builder->inferAggregateDependencies(),
            selectAll: true,
        ));

        $this->assertEquals($direct, $viaSeam);
    }

    public function test_plan_prepared_merges_join_dependencies(): void
    {
        $builder = Post::query()->join('authors', 'authors.id', '=', 'posts.author_id');
        $prepared = $builder->prepareCacheExecution();

        $captured = null;
        $builder->planPrepared($prepared, function (DependencySet $inferred) use ($prepared, &$captured) {
            $captured = $inferred;

            return CachePlanContext::models(
                ProjectionClassifier::resolve($prepared->base, ['*']),
                $inferred,
                selectAll: true,
            );
        });

        $this->assertNotNull($captured);
        $this->assertNotEquals(DependencySet::empty(), $captured);
    }
}
