<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class UnifiedQueryEntryTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);

        foreach (range(1, 4) as $index) {
            RawPost::query()->toBase()->insert([
                'title' => "Post {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    public function test_a_canonical_publish_writes_the_membership_and_overlay_to_one_key(): void
    {
        RawPost::query()->toBase()->orderBy('id')->get();

        $withMembership = $this->cacheQueryKeysWithField('m');

        $this->assertCount(1, $withMembership);
        $this->assertSame(
            $withMembership,
            $this->cacheQueryKeysWithField('r'),
            'the membership and its overlay must land on the same query entry key',
        );
    }

    public function test_an_oversized_result_clears_a_stale_overlay_but_keeps_the_membership(): void
    {
        $query = fn() => RawPost::query()->toBase()->orderBy('id')->get();
        $query();

        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $this->assertNotNull($this->cacheStore()->readHashField($entryKey, 'r'));

        $originalConfig = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['auto_overlay_max_rows'] = 1;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        try {
            // Force a rebuild onto the same key: invalidating would bump the
            // version and publish the membership somewhere else entirely.
            $this->cacheStore()->writeHashField($entryKey, 'r', 'corrupt');
            $this->cacheStore()->deleteHashField($entryKey, 'm');

            $query();

            $this->assertSame([$entryKey], $this->cacheQueryKeysWithField('m'));
            $this->assertNull(
                $this->cacheStore()->readHashField($entryKey, 'r'),
                'a rejected overlay must be cleared from the entry it shares with the membership',
            );
        } finally {
            $this->app->instance(CacheConfig::class, $originalConfig);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_a_query_group_read_costs_a_single_pipelined_round_trip(): void
    {
        $this->skipWhenCommandStatsAreSharded();

        $query = fn() => RawPost::query()->toBase()
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->select('posts.id')
            ->orderBy('posts.id')
            ->get();

        $query();

        $this->assertSame(
            [],
            $this->cacheQueryKeysWithField('m'),
            'a joined query must take the query-group route',
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $calls = $this->commandCallsDuring($query);
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog(), 'the read must still be served from cache');
        $this->assertSame(0, $calls['hget'] ?? 0, 'the entry must not cost a standalone HGET');
        $this->assertSame(0, $calls['mget'] ?? 0, 'the cache state must not cost a second round trip');
    }

    public function test_a_canonical_overlay_read_needs_nothing_beyond_its_script(): void
    {
        $this->skipWhenCommandStatsAreSharded();

        $query = fn() => RawPost::query()->toBase()->orderBy('id')->get();

        $query();

        $calls = $this->commandCallsDuring($query);

        $this->assertSame(0, $calls['hget'] ?? 0);
        $this->assertSame(
            0,
            $calls['mget'] ?? 0,
            'the canonical route resolves state inside fetch_result_or_canonical.lua',
        );
    }

    public function test_promoting_an_overlay_refreshes_the_shared_query_entry_ttl(): void
    {
        $query = fn() => RawPost::query()->toBase()->orderBy('id')->get();
        $query();

        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $redis = Redis::connection('normcache-test');

        $this->cacheStore()->deleteHashField($entryKey, 'r');
        $redis->expire($entryKey, 5);
        $this->assertLessThanOrEqual(5, (int) $redis->ttl($entryKey));

        $query();

        $this->assertGreaterThan(
            5,
            (int) $redis->ttl($entryKey),
            'promoting the overlay also extends the membership: both fields share one key TTL',
        );
        $this->assertNotNull($this->cacheStore()->readHashField($entryKey, 'm'));
        $this->assertNotNull($this->cacheStore()->readHashField($entryKey, 'r'));
    }

    public function test_a_warm_overlay_hit_leaves_the_query_entry_ttl_alone(): void
    {
        $query = fn() => RawPost::query()->toBase()->orderBy('id')->get();
        $query();

        $entryKey = $this->cacheQueryKeysWithField('m')[0];
        $redis = Redis::connection('normcache-test');
        $redis->expire($entryKey, 5);

        $query();

        $this->assertLessThanOrEqual(
            5,
            (int) $redis->ttl($entryKey),
            'only a publication may refresh the entry TTL',
        );
    }

    private function skipWhenCommandStatsAreSharded(): void
    {
        if (env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true) {
            $this->markTestSkipped('A cluster MGET is split per hash tag, so the counts differ.');
        }
    }

    /**
     * Counts client round trips, not commands: Redis also attributes every
     * redis.call() inside a Lua script to its own INFO commandstats entry.
     * Scripts are invoked on the raw phpredis client and stay uncounted here,
     * which is what makes a zero count meaningful for the canonical route.
     *
     * @param  callable(): mixed  $callback
     * @return array<string, int>
     */
    private function commandCallsDuring(callable $callback): array
    {
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $calls = [];

        $connection->listen(static function (CommandExecuted $event) use (&$calls): void {
            $command = strtolower($event->command);
            $calls[$command] = ($calls[$command] ?? 0) + 1;
        });

        try {
            $callback();
        } finally {
            $connection->unsetEventDispatcher();
        }

        return $calls;
    }
}
