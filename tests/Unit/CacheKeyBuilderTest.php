<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Eloquent\Model;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheSpace;

class CacheKeyBuilderTest extends TestCase
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
}
