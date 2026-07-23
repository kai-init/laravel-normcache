<?php

namespace NormCache\Tests\Unit;

use NormCache\Cache\Invalidator;
use NormCache\Cache\ModelCache;
use NormCache\Cache\ModelIndexCache;
use NormCache\Cache\RelationIndexCache;
use NormCache\Cache\ResultCache;
use NormCache\Enums\CacheKind;
use NormCache\Enums\ResultKind;
use NormCache\Payload\ModelIndexAdapter;
use NormCache\Payload\SerializedArrayAdapter;
use NormCache\Planning\QueryEligibility;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheSpace;
use NormCache\Values\SpaceValidationResult;

final class ArchitectureContractTest extends TestCase
{
    public function test_manager_exposes_target_cache_family_services(): void
    {
        $manager = $this->cacheManager();

        $this->assertInstanceOf(ModelCache::class, $manager->modelCache());
        $this->assertInstanceOf(ModelIndexCache::class, $manager->modelIndexes());
        $this->assertInstanceOf(ResultCache::class, $manager->resultCache());
        $this->assertInstanceOf(RelationIndexCache::class, $manager->relationIndexes());
        $this->assertInstanceOf(Invalidator::class, $manager->invalidator());
    }

    public function test_cache_kind_selects_compatible_existing_namespaces(): void
    {
        $keys = new CacheKeyBuilder;

        $this->assertSame(CacheKeyBuilder::K_MODEL, $keys->namespaceFor(CacheKind::Model));
        $this->assertSame(CacheKeyBuilder::K_QUERY, $keys->namespaceFor(CacheKind::ModelIndex));
        $this->assertSame(CacheKeyBuilder::K_THROUGH, $keys->namespaceFor(CacheKind::RelationIndex));
        $this->assertSame(CacheKeyBuilder::K_COUNT, $keys->namespaceFor(CacheKind::Result, ResultKind::Count));
        $this->assertSame(CacheKeyBuilder::K_RESULT, $keys->namespaceFor(CacheKind::Result, ResultKind::Collection));
        $this->assertSame(CacheKeyBuilder::K_SCALAR, $keys->namespaceFor(CacheKind::Result, ResultKind::Value));
        $this->assertSame(CacheKeyBuilder::K_VER, $keys->namespaceFor(CacheKind::Version));
    }

    public function test_cache_space_fit_is_an_explicit_eligibility_policy(): void
    {
        $eligibility = new QueryEligibility;
        $space = new CacheSpace('content', 'nc:content');

        $this->assertTrue($eligibility->fitsCacheSpace(new SpaceValidationResult(true, $space)));
        $this->assertFalse($eligibility->fitsCacheSpace(new SpaceValidationResult(false, $space, ['other'])));
    }

    public function test_payload_adapters_distinguish_empty_and_corrupt_payloads(): void
    {
        $modelIndexes = new ModelIndexAdapter;
        $this->assertTrue($modelIndexes->decode('[]')->valid);
        $this->assertTrue($modelIndexes->decode('[]')->empty);
        $this->assertFalse($modelIndexes->decode('{"id":1}')->valid);

        $store = new RedisStore('normcache-test');
        $pivots = new SerializedArrayAdapter($store);
        $this->assertTrue($pivots->decode($store->serialize([]))->empty);
        $this->assertFalse($pivots->decode('not-serialized')->valid);
    }
}
