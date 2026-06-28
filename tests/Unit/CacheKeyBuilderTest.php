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

    public function test_building_to_wake_key_carves_multi_version_key_without_braces(): void
    {
        $keys = new CacheKeyBuilder;

        $wake = $keys->buildingToWakeKey('building:mysql:posts:v12:v3:v9:abc123');

        $this->assertSame('wake:mysql:posts:abc123', $wake);
    }

    public function test_building_to_wake_key_carves_single_version_key(): void
    {
        $keys = new CacheKeyBuilder;

        $wake = $keys->buildingToWakeKey('building:mysql:posts:v1:deadbeef');

        $this->assertSame('wake:mysql:posts:deadbeef', $wake);
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

    public function test_building_to_wake_key_is_prefix_agnostic_on_full_keys(): void
    {
        $keys = new CacheKeyBuilder;

        $wake = $keys->buildingToWakeKey('{nc}:test:building:mysql:posts:v12:v3:abc123');

        $this->assertSame('{nc}:test:wake:mysql:posts:abc123', $wake);
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
