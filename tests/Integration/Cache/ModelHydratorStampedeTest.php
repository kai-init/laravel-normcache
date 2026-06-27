<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\VersionTracker;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class ModelHydratorStampedeTest extends TestCase
{
    public function test_acquires_lock_fetches_once_caches_and_releases_lock_on_miss(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Alice']);
        $this->evictModelCache(Author::class, $author->id);

        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockSuffix = $keys->resultBuildIdentityHash('model', null, (string) $author->id);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', $lockSuffix);

        DB::enableQueryLog();
        $models = $manager->getModels([$author->id], Author::class);
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertCount(1, $models);
        $this->assertSame('Alice', $models[0]->name);
        $this->assertCount(1, $queries, 'Expected exactly one DB query to hydrate the missed model');

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertNull($manager->getStore()->getRaw($lockKey), 'Building lock must be released after a miss');
    }

    public function test_falls_back_to_database_without_releasing_someone_elses_lock(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Bob']);
        $this->evictModelCache(Author::class, $author->id);

        $keys = new CacheKeyBuilder;
        $classKey = $keys->classKey(Author::class);
        $lockSuffix = $keys->resultBuildIdentityHash('model', null, (string) $author->id);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', $lockSuffix);

        $store = $manager->getStore();
        $this->assertTrue($store->setNxEx($lockKey, 'other-token', 5));

        $models = $manager->getModels([$author->id], Author::class);

        $this->assertCount(1, $models);
        $this->assertSame('Bob', $models[0]->name);

        // The lock is held by another process — we must not release it.
        $this->assertSame('other-token', $store->getRaw($lockKey));

        // Non-owner waiters read from DB but must not write cache — that is the lock owner's job.
        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
    }

    public function test_build_status_script_claims_lock_when_unheld(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKey = $keys->modelPrefix($classKey, 0) . 'missing-id';

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [$modelKey, $lockKey, $keys->verKey($classKey), $keys->wakeKey($classKey, 'test-lock')],
            ['token', '5']
        );

        $this->assertSame('miss', $result[0]);
        $this->assertSame('token', $result[1]);
        $this->assertSame('token', $store->getRaw($lockKey));
    }

    public function test_build_status_script_reports_building_when_lock_already_held(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKey = $keys->modelPrefix($classKey, 0) . 'missing-id';
        $store->setNxEx($lockKey, 'other-token', 5);

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [$modelKey, $lockKey, $keys->verKey($classKey), $keys->wakeKey($classKey, 'test-lock')],
            ['token', '5']
        );

        $this->assertSame('building', $result[0]);
        // phpredis decodes a nested Lua `false` as PHP false; predis decodes the same RESP nil as null.
        $this->assertFalse((bool) $result[1], 'No lock token should be returned when another process holds the lock');
        $this->assertSame('other-token', $store->getRaw($lockKey), 'Must not overwrite a lock held by another process');
    }

    public function test_build_status_script_reports_hit_without_claiming_lock_when_recheck_resolves(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $modelKey = $keys->modelPrefix($classKey, 0) . 'present-id';
        $store->set($modelKey, ['id' => 1, 'name' => 'Present'], 60);

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [$modelKey, $lockKey, $keys->verKey($classKey), $keys->wakeKey($classKey, 'test-lock')],
            ['token', '5']
        );

        $this->assertSame('hit', $result[0]);
        $this->assertFalse((bool) $result[1], 'No lock token should be returned when the recheck finds everything present');
        $this->assertNull($store->getRaw($lockKey), 'Must not claim the build lock when the recheck already resolves the miss');
    }

    public function test_fetch_missed_status_resolves_via_retry_mget_without_claiming_lock(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;
        $versions = new VersionTracker($store, $keys);
        $hydrator = new ModelHydrator($store, $keys, $versions, 3600, false, 5, 200);

        $author = Author::create(['name' => 'Carol']);
        $classKey = $keys->classKey(Author::class);
        $version = $versions->normalizeVersion($store->getRaw($keys->verKey($classKey)));
        $modelKey = $keys->modelPrefix($classKey, $version) . $author->id;
        $store->set($modelKey, $author->getRawOriginal(), 3600);

        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $hits = [];

        $method = new \ReflectionMethod($hydrator, 'fetchMissedStatus');
        $args = [[$author->id], Author::class, $classKey, $version, null, null, $lockKey, $keys->wakeKey($classKey, 'test-lock'), 'token', &$hits];
        [$status, $missed] = $method->invokeArgs($hydrator, $args);

        $this->assertSame('hit', $status);
        $this->assertSame([], $missed);
        $this->assertArrayHasKey($author->id, $hits);
        $this->assertSame('Carol', $hits[$author->id]->name);
        $this->assertNull($store->getRaw($lockKey), 'Must not claim the build lock when the retry MGET already resolves the miss');
    }

    public function test_build_status_script_all_hit_across_multiple_mget_chunks(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');

        // Exceeds the script's internal MGET chunk size (500) to exercise chunk boundaries.
        $count = 1200;
        $modelKeys = [];
        for ($i = 0; $i < $count; $i++) {
            $modelKey = $keys->modelPrefix($classKey, 0) . "present-{$i}";
            $store->set($modelKey, ['id' => $i], 60);
            $modelKeys[] = $modelKey;
        }

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [...$modelKeys, $lockKey, $keys->verKey($classKey), $keys->wakeKey($classKey, 'test-lock')],
            ['token', '5']
        );

        $this->assertSame('hit', $result[0]);
        $this->assertFalse((bool) $result[1]);
        $this->assertCount($count, $result[3]);

        foreach ($result[3] as $i => $raw) {
            $this->assertNotNull($raw, "Expected value at index {$i} to be present");
        }

        $this->assertNull($store->getRaw($lockKey), 'Must not claim the build lock when everything is present');
    }

    public function test_build_status_script_partial_miss_across_chunk_boundary(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');

        // Put the single missing key right at the chunk boundary (index 500, 0-based) so the
        // miss falls in the second internal MGET chunk.
        $count = 600;
        $missingIndex = 500;
        $modelKeys = [];
        for ($i = 0; $i < $count; $i++) {
            $modelKey = $keys->modelPrefix($classKey, 0) . "key-{$i}";
            if ($i !== $missingIndex) {
                $store->set($modelKey, ['id' => $i], 60);
            }
            $modelKeys[] = $modelKey;
        }

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [...$modelKeys, $lockKey, $keys->verKey($classKey), $keys->wakeKey($classKey, 'test-lock')],
            ['token', '5']
        );

        $this->assertSame('miss', $result[0]);
        $this->assertSame('token', $result[1]);
        $this->assertCount($count, $result[3]);
        // phpredis decodes a nested Lua `false` as PHP false; predis decodes the same RESP nil as null.
        $this->assertFalse((bool) $result[3][$missingIndex], 'Missing key must be falsy at its original index');

        foreach ($result[3] as $i => $raw) {
            if ($i !== $missingIndex) {
                $this->assertNotNull($raw, "Expected value at index {$i} to be present");
            }
        }

        $this->assertSame('token', $store->getRaw($lockKey), 'Must claim the build lock when anything is missing');
    }
}
