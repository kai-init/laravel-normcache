<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\Redis;
use NormCache\Cache\VersionedPayloadStore;
use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Payload\ModelIndexAdapter;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class VersionedPayloadStoreTest extends TestCase
{
    public function test_miss_is_stored_and_subsequent_read_hits(): void
    {
        $builds = 0;
        $store = $this->payloadStore();

        $miss = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: function () use (&$builds): array {
                $builds++;

                return [3, 1];
            },
            modelClass: Author::class,
            hash: 'lifecycle-hit',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );
        $hit = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: function () use (&$builds): array {
                $builds++;

                return [9];
            },
            modelClass: Author::class,
            hash: 'lifecycle-hit',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );

        $this->assertSame(CacheStatus::Miss, $miss->status);
        $this->assertSame(['3', '1'], $hit->payload);
        $this->assertSame(CacheStatus::Hit, $hit->status);
        $this->assertSame(1, $builds);
    }

    public function test_empty_payload_has_distinct_empty_status(): void
    {
        $store = $this->payloadStore();

        $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: fn(): array => [],
            modelClass: Author::class,
            hash: 'lifecycle-empty',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );
        $hit = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: fn(): array => [1],
            modelClass: Author::class,
            hash: 'lifecycle-empty',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );

        $this->assertSame([], $hit->payload);
        $this->assertSame(CacheStatus::Empty, $hit->status);
    }

    public function test_corrupt_payload_is_deleted_and_rebuilt(): void
    {
        $store = $this->payloadStore();
        $first = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: fn(): array => [1],
            modelClass: Author::class,
            hash: 'lifecycle-corrupt',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );
        $this->cacheManager()->store()->setRaw($first->key, '{"invalid":true}', 60);

        $rebuilt = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: fn(): array => [2],
            modelClass: Author::class,
            hash: 'lifecycle-corrupt',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );

        $this->assertSame(CacheStatus::Miss, $rebuilt->status);
        $this->assertSame([2], $rebuilt->payload);
    }

    public function test_build_exception_releases_lock_for_next_builder(): void
    {
        $store = $this->payloadStore();

        try {
            $store->getOrBuild(
                adapter: new ModelIndexAdapter,
                build: fn(): never => throw new \RuntimeException('failed build'),
                modelClass: Author::class,
                hash: 'lifecycle-release',
                tag: null,
                depClasses: [],
                depTableKeys: [],
                kind: CacheKind::ModelIndex,
            );
            $this->fail('Expected build exception.');
        } catch (\RuntimeException $exception) {
            $this->assertSame('failed build', $exception->getMessage());
        }

        $retry = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: fn(): array => [4],
            modelClass: Author::class,
            hash: 'lifecycle-release',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );

        $this->assertSame(CacheStatus::Miss, $retry->status);
        $this->assertSame([4], $retry->payload);
    }

    public function test_version_change_during_build_rejects_stale_store(): void
    {
        $manager = $this->cacheManager();
        $store = $this->payloadStore();
        $builds = 0;

        $first = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: function () use ($manager, &$builds): array {
                $builds++;
                $manager->forceFlushModel(Author::class);

                return [1];
            },
            modelClass: Author::class,
            hash: 'lifecycle-stale',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );
        $second = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: function () use (&$builds): array {
                $builds++;

                return [2];
            },
            modelClass: Author::class,
            hash: 'lifecycle-stale',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
        );

        $this->assertSame(CacheStatus::Miss, $first->status);
        $this->assertSame(CacheStatus::Miss, $second->status);
        $this->assertSame([2], $second->payload);
        $this->assertSame(2, $builds);
    }

    public function test_custom_ttl_is_applied_to_stored_payload(): void
    {
        $store = $this->payloadStore();
        $outcome = $store->getOrBuild(
            adapter: new ModelIndexAdapter,
            build: fn(): array => [1],
            modelClass: Author::class,
            hash: 'lifecycle-ttl',
            tag: null,
            depClasses: [],
            depTableKeys: [],
            kind: CacheKind::ModelIndex,
            ttl: 30,
        );

        $ttl = Redis::connection('normcache-test')->ttl($outcome->key);

        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(30, $ttl);
    }

    private function payloadStore(): VersionedPayloadStore
    {
        $manager = $this->cacheManager();

        return new VersionedPayloadStore(
            $manager->store(),
            $manager->keys(),
            $manager->versionStore(),
            $manager->config(),
            $manager->config()->queryTtl,
            5,
            0,
        );
    }
}
