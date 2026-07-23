<?php

namespace NormCache\Tests\Integration\Infrastructure;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class FetchBatchBuildStatusScriptTest extends TestCase
{
    public function test_claims_lock_when_a_model_payload_is_missing(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKey = $keys->modelPrefix($classKey, 0) . 'missing-id';

        $result = $this->fetchStatus([$modelKey], $lockKey, $keys->wakeKey($classKey, 'test-lock'));

        $this->assertSame('miss', $result[0]);
        $this->assertSame('token', $result[1]);
        $this->assertSame('token', $store->getRaw($lockKey));
    }

    public function test_reports_building_without_overwriting_an_existing_lock(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKey = $keys->modelPrefix($classKey, 0) . 'missing-id';
        $store->setNxEx($lockKey, 'other-token', 5);

        $result = $this->fetchStatus([$modelKey], $lockKey, $keys->wakeKey($classKey, 'test-lock'));

        $this->assertSame('building', $result[0]);
        $this->assertFalse((bool) $result[1]);
        $this->assertSame('other-token', $store->getRaw($lockKey));
    }

    public function test_reports_hit_without_claiming_lock_when_payload_is_present(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKey = $keys->modelPrefix($classKey, 0) . 'present-id';
        $store->set($modelKey, ['id' => 1, 'name' => 'Present'], 60);

        $result = $this->fetchStatus([$modelKey], $lockKey, $keys->wakeKey($classKey, 'test-lock'));

        $this->assertSame('hit', $result[0]);
        $this->assertFalse((bool) $result[1]);
        $this->assertNull($store->getRaw($lockKey));
    }

    public function test_all_hit_payloads_are_returned_across_mget_chunks(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKeys = [];

        for ($i = 0; $i < 1200; $i++) {
            $modelKey = $keys->modelPrefix($classKey, 0) . "present-{$i}";
            $store->set($modelKey, ['id' => $i], 60);
            $modelKeys[] = $modelKey;
        }

        $result = $this->fetchStatus($modelKeys, $lockKey, $keys->wakeKey($classKey, 'test-lock'));

        $this->assertSame('hit', $result[0]);
        $this->assertFalse((bool) $result[1]);
        $this->assertCount(1200, $result[3]);
        foreach ($result[3] as $raw) {
            $this->assertNotNull($raw);
        }
        $this->assertNull($store->getRaw($lockKey));
    }

    public function test_partial_miss_is_preserved_across_mget_chunk_boundary(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $missingIndex = 500;
        $modelKeys = [];

        for ($i = 0; $i < 600; $i++) {
            $modelKey = $keys->modelPrefix($classKey, 0) . "key-{$i}";
            if ($i !== $missingIndex) {
                $store->set($modelKey, ['id' => $i], 60);
            }
            $modelKeys[] = $modelKey;
        }

        $result = $this->fetchStatus($modelKeys, $lockKey, $keys->wakeKey($classKey, 'test-lock'));

        $this->assertSame('miss', $result[0]);
        $this->assertSame('token', $result[1]);
        $this->assertCount(600, $result[3]);
        $this->assertFalse((bool) $result[3][$missingIndex]);
        foreach ($result[3] as $index => $raw) {
            if ($index !== $missingIndex) {
                $this->assertNotNull($raw);
            }
        }
        $this->assertSame('token', $store->getRaw($lockKey));
    }

    private function fetchStatus(array $payloadKeys, string $lockKey, string $wakeKey): array
    {
        return $this->cacheManager()->store()->script(
            RedisScripts::get('fetch_batch_build_status'),
            [...$payloadKeys, $lockKey, $wakeKey],
            ['token', '5'],
        );
    }
}
