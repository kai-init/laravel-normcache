<?php

namespace NormCache\Tests\Unit\Spaces;

use Illuminate\Database\Eloquent\Model;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\UnitTestCase;
use NormCache\Traits\Cacheable;

class CacheSpaceRegistryTest extends UnitTestCase
{
    private function registry(int $maxPerModel = 16): CacheSpaceRegistry
    {
        return new CacheSpaceRegistry($maxPerModel);
    }

    public function test_default_space_maps_to_nc_hash_tag(): void
    {
        $default = $this->registry()->defaultSpace();

        $this->assertSame('default', $default->name);
        $this->assertSame('nc', $default->hashTag);
    }

    public function test_named_space_derives_hash_tag_by_convention(): void
    {
        $space = $this->registry()->space('content');

        $this->assertSame('content', $space->name);
        $this->assertSame('nc:content', $space->hashTag);
    }

    public function test_placement_config_overrides_the_hash_tag(): void
    {
        $registry = new CacheSpaceRegistry(16, ['catalog' => ['hash_tag' => 'shard7']]);

        $this->assertSame('shard7', $registry->space('catalog')->hashTag);
        $this->assertSame('nc:content', $registry->space('content')->hashTag);
    }

    public function test_known_spaces_include_default_and_materialized_spaces(): void
    {
        $registry = new CacheSpaceRegistry(16);
        $registry->space('catalog');

        $this->assertSame(
            ['default', 'catalog'],
            array_map(fn($s) => $s->name, $registry->knownSpaces()),
        );
    }

    public function test_known_spaces_include_configured_placement_spaces(): void
    {
        $registry = new CacheSpaceRegistry(16, ['catalog' => ['hash_tag' => 'shard7']]);

        $this->assertSame(
            ['default', 'catalog'],
            array_map(fn($s) => $s->name, $registry->knownSpaces()),
        );
    }

    public function test_invalid_space_name_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->registry()->space('has space');
    }

    public function test_model_without_declaration_falls_back_to_default(): void
    {
        $registry = $this->registry();

        $spaces = $registry->spacesForModel(Author::class);

        $this->assertSame(['default'], array_map(fn($s) => $s->name, $spaces));
        $this->assertTrue($registry->modelAllowedInSpace(Author::class, 'default'));
        $this->assertFalse($registry->modelAllowedInSpace(Author::class, 'content'));
    }

    public function test_model_declaration_drives_membership(): void
    {
        $registry = $this->registry();

        $spaces = $registry->spacesForModel(SpacedPost::class);

        $this->assertSame(['content'], array_map(fn($s) => $s->name, $spaces));
        $this->assertTrue($registry->modelAllowedInSpace(SpacedPost::class, 'content'));
        $this->assertFalse($registry->modelAllowedInSpace(SpacedPost::class, 'default'));
    }

    public function test_model_in_too_many_spaces_throws_on_resolution(): void
    {
        $model = new class extends Model
        {
            use Cacheable;

            protected static array $normCacheSpaces = ['a', 'b', 'c'];
        };

        $this->expectException(\InvalidArgumentException::class);

        $this->registry(maxPerModel: 2)->spacesForModel($model::class);
    }

    public function test_tables_belong_to_the_default_space(): void
    {
        $registry = $this->registry();

        $this->assertSame(['default'], array_map(fn($s) => $s->name, $registry->spacesForTable('mysql:legacy_flags')));
    }

    public function test_single_base_model_dependencies_do_not_need_validation(): void
    {
        $registry = $this->registry();

        $this->assertTrue($registry->dependenciesAreOnlyModel(Author::class, [Author::class], []));
        $this->assertFalse($registry->dependenciesAreOnlyModel(Author::class, [Author::class, SpacedPost::class], []));
        $this->assertFalse($registry->dependenciesAreOnlyModel(Author::class, [Author::class], ['mysql:legacy_flags']));
    }

    public function test_validate_dependencies_passes_when_all_allowed(): void
    {
        $registry = $this->registry();
        $content = $registry->space('content');

        $result = $registry->validateDependencies($content, [SpacedPost::class], []);

        $this->assertTrue($result->isValid);
        $this->assertSame([], $result->invalidModels);
        $this->assertSame([], $result->dependenciesBySpace);
    }

    public function test_validate_dependencies_can_build_map_for_explain(): void
    {
        $registry = $this->registry();
        $content = $registry->space('content');

        $result = $registry->validateDependencies($content, [SpacedPost::class], [], includeDependenciesBySpace: true);

        $this->assertTrue($result->isValid);
        $this->assertSame(['content'], $result->dependenciesBySpace[SpacedPost::class]);
    }

    public function test_validate_dependencies_reports_cross_space_members(): void
    {
        $registry = $this->registry();
        $content = $registry->space('content');

        // Author is default-only; depending on it inside content is invalid.
        $result = $registry->validateDependencies($content, [SpacedPost::class, Author::class], ['mysql:legacy_flags']);

        $this->assertFalse($result->isValid);
        $this->assertSame([Author::class], $result->invalidModels);
        $this->assertSame(['mysql:legacy_flags'], $result->invalidTables);
        $this->assertSame(['default'], $result->dependenciesBySpace[Author::class]);
        $this->assertSame(['content'], $result->dependenciesBySpace[SpacedPost::class]);
    }
}
