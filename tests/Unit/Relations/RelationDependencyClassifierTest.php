<?php

namespace NormCache\Tests\Unit\Relations;

use NormCache\Relations\RelationDependencyClassifier;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\UnitTestCase;

class RelationDependencyClassifierTest extends UnitTestCase
{
    public function test_classifies_simple_hasmany_relation(): void
    {
        $entry = (new RelationDependencyClassifier)->classify((new Author)->posts(), null);

        $this->assertSame(Post::class, $entry->relatedClass);
        $this->assertNull($entry->throughClass);
        $this->assertNull($entry->tableKey);
        $this->assertSame([], $entry->constraintModels);
        $this->assertSame([], $entry->constraintTables);
        $this->assertSame([Post::class], $entry->modelDependencies());
        $this->assertSame([], $entry->tableDependencies());
    }

    public function test_belongs_to_many_relation_includes_pivot_table_key(): void
    {
        $entry = (new RelationDependencyClassifier)->classify((new Author)->tags(), null);

        $this->assertNotNull($entry->tableKey);
        $this->assertContains($entry->tableKey, $entry->tableDependencies());
    }

    public function test_lock_for_update_relation_definition_is_not_classifiable(): void
    {
        $entry = (new RelationDependencyClassifier)->classify((new Author)->lockedPosts(), null);

        $this->assertNull($entry);
    }

    public function test_without_cache_relation_definition_is_not_classifiable(): void
    {
        $entry = (new RelationDependencyClassifier)->classify((new Author)->cacheSkippedPosts(), null);

        $this->assertNull($entry);
    }

    public function test_non_cacheable_related_model_is_not_classifiable(): void
    {
        $entry = (new RelationDependencyClassifier)->classify((new Author)->uncachedPosts(), null);

        $this->assertNull($entry);
    }
}
