<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
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

    public function test_stampede_script_reports_hit_without_returning_model_payload(): void
    {
        $manager = $this->buildManager();
        $store = $manager->getStore();
        $keys = new CacheKeyBuilder;

        $author = Author::create(['name' => 'Carol']);
        $classKey = $keys->classKey(Author::class);
        $modelKey = $keys->modelPrefix($classKey) . $author->id;
        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');

        $attrs = $author->getRawOriginal();
        $store->set($modelKey, $attrs, 3600);

        $result = $store->script(
            RedisScripts::get('fetch_models_with_stampede'),
            [$modelKey, $lockKey, $keys->verKey($classKey)],
            ['token', '5']
        );

        $this->assertSame('hit', $result[0]);
        $this->assertFalse($result[1], 'No lock token should be returned on a hit');

        // The model payload itself is fetched separately via a plain MGET, not via the script.
        [$fetched] = $store->getMany([$modelKey]);
        $this->assertIsArray($fetched);
        $this->assertSame($attrs['name'], $fetched['name']);
    }
}
