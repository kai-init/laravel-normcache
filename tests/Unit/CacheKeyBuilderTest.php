<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Eloquent\Model;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheSpace;

class CacheKeyBuilderTest extends UnitTestCase
{
    public function test_key_methods_emit_the_space_hash_tag(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');
        $content = new CacheSpace('content', 'nc:content');

        $this->assertSame('{nc:content}:test:ver:mysql:posts:', $keys->verKey('mysql:posts', $content));
        $this->assertSame('{nc:content}:test:model:mysql:posts:v3:', $keys->modelPrefix('mysql:posts', 3, $content));
        $this->assertSame('{nc:content}:test:query:mysql:posts:', $keys->queryPrefix('mysql:posts', null, $content));
        $this->assertSame('{nc:content}:test:query:*', $keys->prefixed('query:*', $content));
    }

    public function test_null_space_keeps_the_default_tag(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');

        $this->assertSame('{nc}:test:ver:mysql:posts:', $keys->verKey('mysql:posts'));
    }

    public function test_class_key_can_be_scoped_to_an_effective_connection(): void
    {
        $keys = new CacheKeyBuilder;

        $this->assertSame('testing:authors', $keys->classKey(Author::class));
        $this->assertSame('secondary_testing:authors', $keys->classKey(Author::class, 'secondary_testing'));
    }

    public function test_table_key_strips_an_explicit_sql_alias(): void
    {
        $keys = new CacheKeyBuilder;

        $this->assertSame('testing:authors', $keys->tableKey('testing', 'authors as a'));
        $this->assertSame('authors', CacheKeyBuilder::stripTableAlias('authors as a'));
    }

    public function test_class_key_rejects_connection_name_containing_colon(): void
    {
        $model = new class extends Model
        {
            protected $connection = 'tenant:7';
        };

        $keys = new CacheKeyBuilder;

        $this->expectException(\InvalidArgumentException::class);

        $keys->classKey($model::class);
    }

    public function test_version_keys_are_brace_free(): void
    {
        $keys = new CacheKeyBuilder('', '');

        $this->assertSame('ver:mysql:posts:', $keys->verKey('mysql:posts'));
        $this->assertSame('scheduled:mysql:posts:', $keys->scheduledKey('mysql:posts'));
        $this->assertSame('building:mysql:posts:', $keys->buildingPrefix('mysql:posts'));
        $this->assertSame('wake:mysql:posts:', $keys->wakePrefix('mysql:posts'));
        $this->assertSame('model:mysql:posts:v3:', $keys->modelPrefix('mysql:posts', 3));
    }

    public function test_dep_key_pairs_respects_active_space_in_static_cache(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');
        $content = new CacheSpace('content', 'nc:content');

        [$defaultVer] = $keys->depKeyPairs('mysql:posts', []);
        [$contentVer] = $keys->withSpace($content, fn() => $keys->depKeyPairs('mysql:posts', []));

        $this->assertSame('{nc}:test:ver:mysql:posts:', $defaultVer[0]);
        $this->assertSame('{nc:content}:test:ver:mysql:posts:', $contentVer[0]);
        $this->assertNotSame($defaultVer[0], $contentVer[0], 'same classKey under two spaces must produce distinct version keys');
    }

    public function test_dep_key_pairs_resolves_each_dependency_classes_own_connection(): void
    {
        $keys = new CacheKeyBuilder;
        $secondaryDep = new class extends Model
        {
            protected $connection = 'secondary_testing';
            protected $table = 'secondary_dep_models';
        };

        $expectedDepKey = $keys->classKey($secondaryDep::class);
        $this->assertSame('secondary_testing:secondary_dep_models', $expectedDepKey);

        [$versionKeys] = $keys->depKeyPairs('testing:authors', [$secondaryDep::class]);

        $this->assertContains($keys->verKey($expectedDepKey), $versionKeys);
        $this->assertContains($keys->verKey('testing:authors'), $versionKeys);
    }

    public function test_active_space_is_exposed_and_restored(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');
        $content = new CacheSpace('content', 'nc:content');

        $seen = $keys->withSpace($content, fn() => $keys->activeSpace());

        $this->assertSame($content, $seen);
        $this->assertNull($keys->activeSpace());
    }

    public function test_key_methods_emit_full_keys_with_hash_tag_and_prefix(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');

        $this->assertSame('{nc}:test:ver:mysql:posts:', $keys->verKey('mysql:posts'));
        $this->assertSame('{nc}:test:scheduled:mysql:posts:', $keys->scheduledKey('mysql:posts'));
        $this->assertSame('{nc}:test:building:mysql:posts:', $keys->buildingPrefix('mysql:posts'));
        $this->assertSame('{nc}:test:wake:mysql:posts:', $keys->wakePrefix('mysql:posts'));
        $this->assertSame('{nc}:test:model:mysql:posts:v3:', $keys->modelPrefix('mysql:posts', 3));
        $this->assertSame('{nc}:test:query:mysql:posts:', $keys->queryPrefix('mysql:posts'));
        $this->assertSame('{nc}:test:query:*', $keys->prefixed('query:*'));
    }

    public function test_result_build_identity_uses_xxh128(): void
    {
        $keys = new CacheKeyBuilder;
        $hash = $keys->resultBuildIdentityHash('scalar', 'report', 'query-hash');

        $this->assertSame(32, strlen($hash));
        $this->assertSame(hash('xxh128', 'scalar:report:query-hash'), $hash);
    }
}
