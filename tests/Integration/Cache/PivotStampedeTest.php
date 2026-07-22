<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Enums\CacheStatus;
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
        $pivotTableKey = $manager->keys()->tableKey($author->getConnection()->getName(), 'author_tag');

        $first = $manager->relationIndexes()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', $pivotTableKey);
        $second = $manager->relationIndexes()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', $pivotTableKey);

        $this->assertSame(CacheStatus::Miss, $first->status);
        $this->assertNotNull($first->build->buildingKey);
        $this->assertSame(CacheStatus::Building, $second->status);
    }

    public function test_pivot_build_lock_is_released_after_store_and_visible_to_next_fetch(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $pivotTableKey = $manager->keys()->tableKey($author->getConnection()->getName(), 'author_tag');

        $miss = $manager->relationIndexes()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', $pivotTableKey);
        $this->assertSame(CacheStatus::Miss, $miss->status);

        $keys = $manager->keys();
        $pivotKey = $keys->pivotKey($keys->classKey(Author::class), $keys->classKey(Tag::class), 'tags', 'nc', $miss->seg, $author->id);

        $manager->relationIndexes()->storePivotEntries(
            [$pivotKey => [['id' => $tag->id, 'pivot' => ['author_id' => $author->id, 'tag_id' => $tag->id]]]],
            null,
            $miss->build,
            Tag::class,
        );

        $this->assertNull($manager->store()->getRaw($miss->build->buildingKey), 'Building lock must be released after store');

        $hit = $manager->relationIndexes()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', $pivotTableKey);
        $this->assertSame(CacheStatus::Hit, $hit->status);
        $this->assertSame([$tag->id], array_column($hit->data[$author->id], 'id'));
    }

    public function test_pivot_falls_back_to_database_without_releasing_someone_elses_lock(): void
    {
        $manager = $this->buildManager(buildingLockTtl: 5, stampedeWaitMs: 50);
        $author = Author::create(['name' => 'Bob']);
        Tag::create(['name' => 'Fiction']);
        $pivotTableKey = $manager->keys()->tableKey($author->getConnection()->getName(), 'author_tag');

        $miss = $manager->relationIndexes()->fetchPivot(Author::class, Tag::class, 'tags', [$author->id], 'nc', $pivotTableKey);
        $this->assertSame(CacheStatus::Miss, $miss->status);
        $store = $manager->store();

        $store->delete($miss->build->buildingKey);
        $this->assertTrue($store->setNxEx($miss->build->buildingKey, 'other-token', 5));

        $waited = $manager->relationIndexes()->waitForPivotBuild(Author::class, Tag::class, 'tags', [$author->id], 'nc', $pivotTableKey);

        $this->assertNull($waited);
        $this->assertSame('other-token', $store->getRaw($miss->build->buildingKey));
    }
}
