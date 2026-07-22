<?php

namespace NormCache\Tests\Unit\Cache;

use NormCache\Cache\VersionStore;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheSpace;

class VersionStoreSpaceTest extends TestCase
{
    public function test_current_version_reads_the_space_scoped_version_key(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');
        $store = new RedisStore('normcache-test');
        $versions = new VersionStore($store, $keys);

        $content = new CacheSpace('content', 'nc:content');
        $classKey = $keys->classKey(Post::class);

        $store->setRaw($keys->verKey($classKey, $content), '7', 60);

        $this->assertSame(7, $versions->currentVersion(Post::class, $content));
        $this->assertSame(0, $versions->currentVersion(Post::class));
    }

    public function test_build_lock_tokens_are_random_128_bit_values(): void
    {
        $versions = new VersionStore(
            new RedisStore('normcache-test'),
            new CacheKeyBuilder,
        );

        $first = $versions->buildLockToken();
        $second = $versions->buildLockToken();

        $this->assertMatchesRegularExpression('/^[a-f0-9]{32}$/', $first);
        $this->assertMatchesRegularExpression('/^[a-f0-9]{32}$/', $second);
        $this->assertNotSame($first, $second);
    }
}
