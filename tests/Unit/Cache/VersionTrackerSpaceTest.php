<?php

namespace NormCache\Tests\Unit\Cache;

use NormCache\Cache\VersionTracker;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheSpace;

class VersionTrackerSpaceTest extends TestCase
{
    public function test_current_version_reads_the_space_scoped_version_key(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');
        $store = new RedisStore('normcache-test');
        $tracker = new VersionTracker($store, $keys);

        $content = new CacheSpace('content', 'nc:content');
        $classKey = $keys->classKey(Post::class);

        $store->setRaw($keys->verKey($classKey, $content), '7', 60);

        $this->assertSame(7, $tracker->currentVersion(Post::class, $content));
        // The default space's version key was never seeded.
        $this->assertSame(0, $tracker->currentVersion(Post::class));
    }
}
