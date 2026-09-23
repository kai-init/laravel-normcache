<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\TableIdentity;

final class LazyOverlaySentinelTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $rows = [];

        for ($index = 1; $index <= 128; $index++) {
            $rows[] = [
                'title' => str_repeat('x', 48) . $index,
                'views' => $index,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        RawPost::query()->toBase()->insert($rows);
    }

    public function test_medium_result_uses_empty_result_as_deferred_sentinel(): void
    {
        $query = $this->cachedQuery();

        $this->assertCount(128, $query());
        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $this->assertSame('', $this->cacheStore()->readHashField($entryKey, 'r'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(128, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        $this->assertNotSame('', $this->cacheStore()->readHashField($entryKey, 'r'));
    }

    public function test_promotion_does_not_extend_ttl(): void
    {
        $query = $this->cachedQuery();
        $query();

        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $redis = Redis::connection('normcache-test');
        $redis->expire($entryKey, 5);

        $query();

        $this->assertLessThanOrEqual(5, (int) $redis->ttl($entryKey));
    }

    public function test_generation_change_blocks_promotion(): void
    {
        $this->cachedQuery()();

        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $generationKey = $this->cacheKeys()->generation($this->postsIdentity());
        $generation = $this->cacheStore()->getRaw($generationKey) ?? '0';

        $this->cacheStore()->increment($generationKey);

        $this->assertFalse($this->cacheStore()->promoteResultOverlay(
            generationKey: $generationKey,
            entryKey: $entryKey,
            expectedGeneration: $generation,
            payload: 'stale',
        ));
        $this->assertSame('', $this->cacheStore()->readHashField($entryKey, 'r'));
    }

    public function test_epoch_race_can_write_but_cannot_serve_stale_overlay(): void
    {
        $rows = $this->cachedQuery()();
        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $membership = $this->cacheStore()->readHashField($entryKey, 'm');
        $this->assertIsString($membership);

        $decoded = app(MembershipCodec::class)->decode($membership);
        $this->assertTrue($decoded->valid);

        $payload = app(RawResultCodec::class)->encode($rows->all(), $decoded->epoch);
        $this->assertIsString($payload);

        $generationKey = $this->cacheKeys()->generation($this->postsIdentity());
        $generation = $this->cacheStore()->getRaw($generationKey) ?? '0';

        NormCache::flushAll();

        $this->assertTrue($this->cacheStore()->promoteResultOverlay(
            generationKey: $generationKey,
            entryKey: $entryKey,
            expectedGeneration: $generation,
            payload: $payload,
        ));

        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");

        DB::flushQueryLog();
        DB::enableQueryLog();
        $fresh = $this->cachedQuery()();
        DB::disableQueryLog();

        $this->assertNotSame([], DB::getQueryLog());
        $this->assertSame('Changed', $fresh->first()->title);
    }

    private function cachedQuery(): callable
    {
        return fn() => RawPost::query()->toBase()->orderBy('id')->get();
    }

    private function postsIdentity(): TableIdentity
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);

        return $identity;
    }
}
