<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Enums\LuaStatus;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use NormCache\Values\ModelFetchContext;

class ModelCacheStampedeTest extends TestCase
{
    public function test_acquires_lock_fetches_once_caches_and_releases_lock_on_miss(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Alice']);
        $this->evictModelCache(Author::class, $author->id);

        $keys = $manager->keys();
        $classKey = $keys->classKey(Author::class);
        $modelVersion = $manager->currentVersion(Author::class);
        $lockSegment = 'model:v' . $modelVersion;
        $lockSuffix = $keys->resultBuildIdentityHash($lockSegment, null, (string) $author->id);
        $lockKey = $keys->resultBuildingKey($classKey, $lockSegment, $lockSuffix);

        DB::enableQueryLog();
        $models = $manager->modelCache()->getModels([$author->id], Author::class);
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertCount(1, $models);
        $this->assertSame('Alice', $models[0]->name);
        $this->assertCount(1, $queries, 'Expected exactly one DB query to hydrate the missed model');
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
        $this->assertNull($manager->store()->getRaw($lockKey), 'Building lock must be released after a miss');
    }

    public function test_falls_back_to_database_without_releasing_someone_elses_lock(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Bob']);
        $this->evictModelCache(Author::class, $author->id);

        $keys = $manager->keys();
        $classKey = $keys->classKey(Author::class);
        $modelVersion = $manager->currentVersion(Author::class);
        $lockSegment = 'model:v' . $modelVersion;
        $lockSuffix = $keys->resultBuildIdentityHash($lockSegment, null, (string) $author->id);
        $lockKey = $keys->resultBuildingKey($classKey, $lockSegment, $lockSuffix);

        $store = $manager->store();
        $this->assertTrue($store->setNxEx($lockKey, 'other-token', 5));

        $models = $manager->modelCache()->getModels([$author->id], Author::class);

        $this->assertCount(1, $models);
        $this->assertSame('Bob', $models[0]->name);
        $this->assertSame('other-token', $store->getRaw($lockKey));
        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
    }

    public function test_retry_mget_resolves_a_concurrent_fill_without_claiming_lock(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = $manager->keys();
        $versions = $manager->versionStore();
        $modelCache = $manager->modelCache();

        $author = Author::create(['name' => 'Carol']);
        $classKey = $keys->classKey(Author::class);
        $version = $versions->normalizeVersion($store->getRaw($keys->verKey($classKey)));
        $modelKey = $keys->modelPrefix($classKey, $version) . $author->id;
        $store->set($modelKey, $author->getRawOriginal(), 3600);

        $lockKey = $keys->resultBuildingKey($classKey, 'model', 'test-lock');
        $context = new ModelFetchContext(
            modelClass: Author::class,
            classKey: $classKey,
            projection: null,
            prototype: null,
            missedQuery: null,
            preserveQueryShape: true,
            modelVersion: $version,
        );
        $context->lockKey = $lockKey;
        $context->wakeKey = $keys->wakeKey($classKey, 'test-lock');
        $context->token = 'token';

        $method = new \ReflectionMethod($modelCache, 'fetchMissedStatus');
        [$status, $missed] = $method->invokeArgs($modelCache, [[$author->id], $context]);

        $this->assertSame(LuaStatus::Hit, $status);
        $this->assertSame([], $missed);
        $this->assertArrayHasKey($author->id, $context->hits);
        $this->assertSame('Carol', $context->hits[$author->id]->name);
        $this->assertNull($store->getRaw($lockKey));
    }
}
