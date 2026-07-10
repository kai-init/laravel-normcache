<?php

namespace NormCache\Tests\Unit;

use NormCache\Enums\CacheOperation;
use NormCache\Enums\CacheStrategy;
use NormCache\Planning\CachePlanSpaceValidator;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Tests\Fixtures\Models\CatalogTag;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlan;
use NormCache\Values\DependencySet;

class CachePlanSpaceValidatorTest extends UnitTestCase
{
    public function test_cross_space_dependencies_bypass_with_the_resolved_space(): void
    {
        $registry = new CacheSpaceRegistry;
        $validator = new CachePlanSpaceValidator($registry, new CacheSpaceResolver($registry));
        $builder = SpacedPost::query();
        $plan = CachePlan::result(
            CacheOperation::Models,
            new DependencySet(models: [SpacedPost::class, CatalogTag::class]),
        );

        $validated = $validator->validate($plan, $builder, $builder->getModel());

        $this->assertSame(CacheStrategy::LiveQuery, $validated->strategy);
        $this->assertSame('content', $validated->space?->name);
        $this->assertStringContainsString(CatalogTag::class, $validated->bypassReasons['dependency'][0]);
    }

    public function test_cross_space_dependencies_can_throw(): void
    {
        $registry = new CacheSpaceRegistry;
        $validator = new CachePlanSpaceValidator(
            $registry,
            new CacheSpaceResolver($registry),
            crossSpaceBehavior: 'throw',
        );
        $builder = SpacedPost::query();
        $plan = CachePlan::result(
            CacheOperation::Models,
            new DependencySet(models: [SpacedPost::class, CatalogTag::class]),
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('cross-space dependencies for space [content]');

        $validator->validate($plan, $builder, $builder->getModel());
    }
}
