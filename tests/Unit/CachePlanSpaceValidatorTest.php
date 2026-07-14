<?php

namespace NormCache\Tests\Unit;

use Illuminate\Redis\Connections\PredisConnection;
use NormCache\Enums\CacheOperation;
use NormCache\Enums\CacheStrategy;
use NormCache\Planning\CachePlanSpaceValidator;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\CatalogTag;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CachePlan;
use NormCache\Values\DependencySet;
use ReflectionProperty;
use RuntimeException;

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
        $this->assertStringContainsString(CatalogTag::class, $validated->bypassReasons['space'][0]);
        $this->assertArrayNotHasKey('dependency', $validated->bypassReasons);
    }

    public function test_failed_table_space_registration_uses_space_bypass_category(): void
    {
        $connection = new class extends PredisConnection
        {
            public function __construct() {}

            public function command($method, array $parameters = [])
            {
                return match (strtolower($method)) {
                    'smembers' => [],
                    'sadd' => throw new RuntimeException('SADD denied'),
                    default => null,
                };
            }
        };
        $store = new RedisStore('normcache-test');
        (new ReflectionProperty(RedisStore::class, 'connection'))->setValue($store, $connection);

        $registry = new CacheSpaceRegistry(metadataStore: $store);
        $validator = new CachePlanSpaceValidator($registry, new CacheSpaceResolver($registry));
        $builder = SpacedPost::query();
        $plan = CachePlan::result(
            CacheOperation::Models,
            new DependencySet(models: [SpacedPost::class], tables: ['testing:authors']),
        );

        $validated = $validator->validate($plan, $builder, $builder->getModel());

        $this->assertSame(
            ['failed to register table-space dependencies'],
            $validated->bypassReasons['space'],
        );
        $this->assertArrayNotHasKey('dependency', $validated->bypassReasons);
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
