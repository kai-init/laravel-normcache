<?php

namespace NormCache\Tests\Integration\Infrastructure;

use Illuminate\Support\Facades\Redis;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;

/**
 * Version key TTL behavior — protects against Redis LRU eviction dropping a
 * version key while its payloads are still alive.
 */
class VersionKeyTtlTest extends TestCase
{
    public function test_version_key_has_ttl_after_invalidation(): void
    {
        UuidItem::create(['id' => 'aaaaaaaa-0000-0000-0000-000000000001', 'name' => 'Alpha']);

        $redis = Redis::connection('normcache-test');
        $classKey = $this->cacheManager()->keys()->classKey(UuidItem::class);
        $verKey = '{nc}:test:ver:' . $classKey . ':';

        $ttl = $redis->ttl($verKey);

        // Version key must have an explicit TTL (not -1 = no TTL, not -2 = does not exist).
        $this->assertGreaterThan(0, $ttl, 'version key must have a positive TTL to survive Redis LRU eviction');
    }

    public function test_version_key_ttl_exceeds_longest_payload_ttl(): void
    {
        UuidItem::create(['id' => 'aaaaaaaa-0000-0000-0000-000000000001', 'name' => 'Alpha']);

        $redis = Redis::connection('normcache-test');
        $classKey = $this->cacheManager()->keys()->classKey(UuidItem::class);
        $verKey = '{nc}:test:ver:' . $classKey . ':';

        $verTtl = $redis->ttl($verKey);
        $modelTtl = (int) config('normcache.ttl');
        $queryTtl = (int) config('normcache.query_ttl');

        // Version TTL must exceed the longest payload TTL so version keys outlive their payloads.
        $this->assertGreaterThanOrEqual(max($modelTtl, $queryTtl), $verTtl);
    }
}
