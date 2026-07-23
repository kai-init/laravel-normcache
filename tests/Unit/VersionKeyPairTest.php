<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheSpace;

class VersionKeyPairTest extends UnitTestCase
{
    public function test_it_builds_version_and_scheduled_keys_for_a_space(): void
    {
        $keys = new CacheKeyBuilder('{nc}:', 'test:');
        $space = new CacheSpace('content', 'nc:content');

        $this->assertSame([
            '{nc:content}:test:ver:mysql:posts:',
            '{nc:content}:test:scheduled:mysql:posts:',
        ], $keys->versionKeyPair('mysql:posts', $space));
    }
}
