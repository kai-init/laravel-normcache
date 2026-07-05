<?php

namespace NormCache\Tests\Unit;

use NormCache\Cache\ModelsExecutor;
use NormCache\CacheableBuilder;
use NormCache\Planning\CachePlanner;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class CacheableBuilderSharedStateTest extends TestCase
{
    public function test_reset_clears_shared_builder_state(): void
    {
        $planner = new \ReflectionProperty(CacheableBuilder::class, 'sharedPlanner');
        $executor = new \ReflectionProperty(CacheableBuilder::class, 'sharedModelsExecutor');

        $planner->setValue(null, new CachePlanner);
        $executor->setValue(null, new ModelsExecutor);

        CacheKeyBuilder::reset();

        $this->assertNull($planner->getValue());
        $this->assertNull($executor->getValue());
    }

    public function test_builders_share_one_planner_instance(): void
    {
        $this->assertSame(Author::query()->planner(), Post::query()->planner());
    }
}
