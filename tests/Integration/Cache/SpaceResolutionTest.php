<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CachePlanContext;

class SpaceResolutionTest extends TestCase
{
    public function test_space_method_sets_and_reads_back_the_space(): void
    {
        $builder = Post::query()->space('content');

        $this->assertSame('content', $builder->getSpace());
    }

    public function test_space_is_null_by_default(): void
    {
        $this->assertNull(Post::query()->getSpace());
    }

    public function test_cross_space_dependency_bypasses(): void
    {
        $plan = SpacedPost::query()
            ->dependsOn([Author::class])
            ->cachePlan(SpacedPost::query()->toBase(), CachePlanContext::models());

        $this->assertFalse($plan->isCacheable(), 'SpacedPost(content) depending on Author(default) must bypass');
        $this->assertTrue($plan->hasBypassReason('dependency'));
    }

    public function test_same_space_dependency_still_caches(): void
    {
        // No declared spaces on Post/Author → both in default → no cross-space bypass.
        $plan = Post::query()
            ->dependsOn([Author::class])
            ->cachePlan(Post::query()->toBase(), CachePlanContext::models());

        $this->assertTrue($plan->isCacheable(), 'default-space deps must not be downgraded');
    }
}
