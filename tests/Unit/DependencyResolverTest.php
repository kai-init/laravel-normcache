<?php

namespace NormCache\Tests\Unit;

use NormCache\Enums\CacheOperation;
use NormCache\Planning\DependencyResolver;
use NormCache\Planning\QueryInspection;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;

class DependencyResolverTest extends UnitTestCase
{
    public function test_explicit_dependency_does_not_mask_an_unsafe_inferred_dependency(): void
    {
        $inspection = new QueryInspection(
            dependencies: DependencySet::unsafe('joined subquery dependency could not be inferred'),
        );

        $resolved = (new DependencyResolver)->resolve(
            modelClass: Post::class,
            context: new CachePlanContext(CacheOperation::Models),
            inspection: $inspection,
            explicitModels: [Author::class],
            explicitTables: [],
            hasExplicit: true,
        );

        $this->assertFalse(
            $resolved->safe,
            'An explicit dependsOn() must not silently discard an unresolvable inferred dependency.'
        );
        $this->assertContains('joined subquery dependency could not be inferred', $resolved->reasons);
    }

    public function test_explicit_dependency_with_safe_inference_stays_safe(): void
    {
        $resolved = (new DependencyResolver)->resolve(
            modelClass: Post::class,
            context: new CachePlanContext(CacheOperation::Models),
            inspection: new QueryInspection,
            explicitModels: [Author::class],
            explicitTables: [],
            hasExplicit: true,
        );

        $this->assertTrue($resolved->safe);
        $this->assertSame([Post::class, Author::class], $resolved->models);
    }
}
