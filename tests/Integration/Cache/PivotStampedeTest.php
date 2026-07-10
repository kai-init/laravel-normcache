<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Enums\CacheStatus;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

class PivotStampedeTest extends TestCase
{
    public function test_concurrent_pivot_miss_second_caller_sees_building_status(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Alice']);
        Tag::create(['name' => 'Fiction']);

        $first = $manager->results()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', null);
        $second = $manager->results()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', null);

        $this->assertSame(CacheStatus::Miss, $first->status);
        $this->assertNotNull($first->build->buildingKey);
        $this->assertSame(CacheStatus::Building, $second->status);
    }

    public function test_pivot_build_lock_is_released_after_store_and_visible_to_next_fetch(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);

        $miss = $manager->results()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', null);
        $this->assertSame(CacheStatus::Miss, $miss->status);

        $keys = $manager->keys();
        $pivotKey = $keys->pivotKey($keys->classKey(Author::class), $keys->classKey(Tag::class), 'tags', 'nc', $miss->seg, $author->id);

        $manager->results()->storeMany(
            [$pivotKey => [['id' => $tag->id, 'pivot' => ['author_id' => $author->id, 'tag_id' => $tag->id]]]],
            build: $miss->build,
        );

        $this->assertNull($manager->store()->getRaw($miss->build->buildingKey), 'Building lock must be released after store');

        $hit = $manager->results()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', null);
        $this->assertSame(CacheStatus::Hit, $hit->status);
        $this->assertSame([$tag->id], array_column($hit->data[$author->id], 'id'));
    }

    public function test_pivot_falls_back_to_database_without_releasing_someone_elses_lock(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Bob']);
        Tag::create(['name' => 'Fiction']);

        $miss = $manager->results()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', null);
        $this->assertSame(CacheStatus::Miss, $miss->status);
        $store = $manager->store();

        // Someone else now holds the lock under a different token.
        $store->delete($miss->build->buildingKey);
        $this->assertTrue($store->setNxEx($miss->build->buildingKey, 'other-token', 5));

        $waited = $manager->results()->waitForPivotBuild(Author::class, Tag::class, 'tags', [$author->id], 'nc', null);

        $this->assertNull($waited, 'Waiting must time out and report null while the lock is held by someone else');
        $this->assertSame('other-token', $store->getRaw($miss->build->buildingKey), 'Must not release a lock held by another process');
    }

    public function test_pivot_build_status_script_claims_lock_when_unheld(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;

        $lockKey = $keys->resultBuildingKey('cls', 'v1', 'test-lock');
        $wakeKey = $keys->wakeKey('cls', 'test-lock');
        $pivotKey = $keys->prefixed('pivot:missing:1');

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [$pivotKey, $lockKey, $wakeKey],
            ['token', '5']
        );

        $this->assertSame('miss', $result[0]);
        $this->assertSame('token', $result[1]);
        $this->assertSame('token', $store->getRaw($lockKey));
    }

    public function test_pivot_build_status_script_reports_building_when_lock_already_held(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;

        $lockKey = $keys->resultBuildingKey('cls', 'v1', 'test-lock');
        $wakeKey = $keys->wakeKey('cls', 'test-lock');
        $pivotKey = $keys->prefixed('pivot:missing:1');
        $store->setNxEx($lockKey, 'other-token', 5);

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [$pivotKey, $lockKey, $wakeKey],
            ['token', '5']
        );

        $this->assertSame('building', $result[0]);
        $this->assertFalse((bool) $result[1]);
        $this->assertSame('other-token', $store->getRaw($lockKey), 'Must not overwrite a lock held by another process');
    }

    public function test_pivot_build_status_script_reports_hit_without_claiming_lock_when_recheck_resolves(): void
    {
        $manager = $this->buildManager();
        $store = $manager->store();
        $keys = new CacheKeyBuilder;

        $lockKey = $keys->resultBuildingKey('cls', 'v1', 'test-lock');
        $wakeKey = $keys->wakeKey('cls', 'test-lock');
        $pivotKey = $keys->prefixed('pivot:present:1');
        $store->setRaw($pivotKey, $store->serialize([['id' => 1]]), 60);

        $result = $store->script(
            RedisScripts::get('fetch_batch_build_status'),
            [$pivotKey, $lockKey, $wakeKey],
            ['token', '5']
        );

        $this->assertSame('hit', $result[0]);
        $this->assertFalse((bool) $result[1]);
        $this->assertNull($store->getRaw($lockKey), 'Must not claim the build lock when the recheck already resolves the miss');
    }
}
