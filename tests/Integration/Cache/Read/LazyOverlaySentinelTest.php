<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Facades\NormCache;
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
                'title' => str_repeat('x', 512) . $index,
                'views' => $index,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        RawPost::query()->toBase()->insert($rows);
    }

    public function test_medium_membership_has_no_overlay_on_cold_or_repeated_reads(): void
    {
        $query = $this->cachedQuery();

        $this->assertCount(128, $query());
        $entryKey = $this->membershipKey();
        $this->assertNull($this->cacheStore()->readHashField($entryKey, 'r'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(128, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        $this->assertNull($this->cacheStore()->readHashField($entryKey, 'r'));
    }

    public function test_missing_canonical_row_is_selectively_repaired_in_membership_order(): void
    {
        $query = $this->cachedQuery();
        $expected = $query()->pluck('id')->all();
        $entryKey = $this->membershipKey();
        $missingId = $expected[63];
        $this->cacheStore()->delete($this->cacheKeys()->row(
            $this->postsIdentity(),
            $this->cacheStore()->getRaw($this->cacheKeys()->generation($this->postsIdentity())) ?? '0',
            'i:' . $missingId,
        ));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $query()->pluck('id')->all();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame($expected, $actual);
        $this->assertCount(1, $queries);
        $this->assertSame([$missingId], $queries[0]['bindings']);
        $this->assertStringContainsString(' in (', $queries[0]['query']);
        $this->assertSame([], $this->cacheQueryKeysWithField('r'));
        $this->assertSame($entryKey, $this->membershipKey());
    }

    public function test_repeated_medium_membership_reads_do_not_extend_ttl(): void
    {
        $query = $this->cachedQuery();
        $query();

        $entryKey = $this->membershipKey();
        $redis = Redis::connection('normcache-test');
        $redis->expire($entryKey, 20);
        $before = (int) $redis->ttl($entryKey);

        $query();

        $this->assertLessThanOrEqual($before, (int) $redis->ttl($entryKey));
    }

    public function test_public_table_invalidation_prevents_stale_membership_data(): void
    {
        $query = $this->cachedQuery();
        $query();
        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");

        NormCache::invalidate(['posts']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $fresh = $query();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotSame([], $queries);
        $this->assertSame('Changed', $fresh->first()->title);
    }

    public function test_small_canonical_result_publishes_eager_overlay(): void
    {
        $query = fn() => RawPost::query()->toBase()->orderBy('id')->limit(1)->get();

        $query();

        $entryKey = $this->membershipKey();
        $this->assertNotNull($this->cacheStore()->readHashField($entryKey, 'r'));
        $this->assertNotNull($this->cacheStore()->readHashField($entryKey, 'm'));
    }

    public function test_tagged_medium_canonical_result_publishes_eager_overlay(): void
    {
        $query = fn() => RawPost::query()->toBase()->orderBy('id')->tag('medium')->get();

        $query();

        $entryKey = $this->membershipKey();
        $overlay = $this->cacheStore()->readHashField($entryKey, 'r');
        $this->assertIsString($overlay);
        $this->assertGreaterThan(32 * 1024, strlen($overlay));
        $this->assertLessThanOrEqual(128 * 1024, strlen($overlay));
        $this->assertNotNull($this->cacheStore()->readHashField($entryKey, 'm'));
    }

    public function test_legacy_empty_overlay_sentinel_reads_membership_without_publishing_overlay(): void
    {
        $query = $this->cachedQuery();
        $query();
        $entryKey = $this->membershipKey();
        $this->writeCacheField($entryKey, 'r', '');

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(128, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        $this->assertSame('', $this->cacheStore()->readHashField($entryKey, 'r'));
    }

    private function cachedQuery(): callable
    {
        return fn() => RawPost::query()->toBase()->orderBy('id')->get();
    }

    private function membershipKey(): string
    {
        $key = $this->cacheQueryKeysWithField('m')[0] ?? null;
        $this->assertIsString($key);

        return $key;
    }

    private function postsIdentity(): TableIdentity
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);

        return $identity;
    }
}
