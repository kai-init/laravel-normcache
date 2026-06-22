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
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50, slotting: true);
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
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50, slotting: true);
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

        // Our DB fallback still populates the cache for future requests.
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
    }

    public function test_build_status_script_claims_lock_when_unheld(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $classKey = $keys->classKey(Author::class);
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');

        $result = $store->script(
            RedisScripts::get('fetch_model_build_status'),
            [$lockKey, $keys->verKey($classKey)],
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
        $store->setNxEx($lockKey, 'other-token', 5);

        $result = $store->script(
            RedisScripts::get('fetch_model_build_status'),
            [$lockKey, $keys->verKey($classKey)],
            ['token', '5']
        );

        $this->assertSame('building', $result[0]);
        // phpredis decodes a nested Lua `false` as PHP false; predis decodes the same RESP nil as null.
        $this->assertFalse((bool) $result[1], 'No lock token should be returned when another process holds the lock');
        $this->assertSame('other-token', $store->getRaw($lockKey), 'Must not overwrite a lock held by another process');
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
        $modelKey = $keys->modelPrefix($classKey) . $author->id;
        $store->set($modelKey, $author->getRawOriginal(), 3600);

        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $hits = [];

        $method = new \ReflectionMethod($hydrator, 'fetchMissedStatus');
        $args = [[$author->id], Author::class, $classKey, null, null, $lockKey, 'token', &$hits];
        [$status, $missed] = $method->invokeArgs($hydrator, $args);

        $this->assertSame('hit', $status);
        $this->assertSame([], $missed);
        $this->assertArrayHasKey($author->id, $hits);
        $this->assertSame('Carol', $hits[$author->id]->name);
        $this->assertNull($store->getRaw($lockKey), 'Must not claim the build lock when the retry MGET already resolves the miss');
    }
}
