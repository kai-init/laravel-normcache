<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class OptimizationsTest extends TestCase
{
    public function test_fast_path_is_used_for_primary_key_lookup()
    {
        $author = Author::create(['name' => 'Fast Path Author']);

        // Clear all events
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        // This should trigger the fast path
        $found = Author::where('id', $author->id)->get();

        $this->assertCount(1, $found);
        $this->assertEquals('Fast Path Author', $found->first()->name);

        // Verify NO QueryCache events were fired
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_fast_path_is_used_for_where_in_primary_key()
    {
        $a1 = Author::create(['name' => 'A1']);
        $a2 = Author::create(['name' => 'A2']);

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        $found = Author::whereIn('id', [$a1->id, $a2->id])->get();

        $this->assertCount(2, $found);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_lua_retrieval_stores_json_and_fetches_in_one_go()
    {
        Author::create(['name' => 'Lua Author']);

        // First query to populate cache
        Author::where('name', 'Lua Author')->get();

        // Check Redis for the query key format
        $redis = Redis::connection(config('normcache.connection'));
        $prefix = config('normcache.key_prefix');

        // Find the query key
        $keys = $redis->keys($prefix . 'query:*');
        $this->assertNotEmpty($keys);

        $value = $redis->get($keys[0]);
        // It should be JSON array
        $this->assertStringStartsWith('[', $value);
        $this->assertStringEndsWith(']', $value);

        // Second query should hit via Lua
        Event::fake([QueryCacheHit::class]);
        $found = Author::where('name', 'Lua Author')->get();

        $this->assertCount(1, $found);
        Event::assertDispatched(QueryCacheHit::class);
    }

    public function test_query_hit_fast_path_returns_model_payload_arrays(): void
    {
        $author = Author::create(['name' => 'Payload Author']);

        Author::where('name', 'Payload Author')->get();

        $query = Author::where('name', 'Payload Author');
        $base = $query->toBase();
        $base->columns = null;
        $hash = QueryHasher::fromQuery($base);
        $cacheData = app('normcache')->getModelsFromQuery(Author::class, $hash);

        $this->assertSame([$author->id], $cacheData['ids']);
        $this->assertIsArray($cacheData['models']);
        $this->assertIsArray($cacheData['models'][0]);
        $this->assertSame('Payload Author', $cacheData['models'][0]['name']);
    }

    public function test_corrupt_query_cache_payload_degrades_to_miss_and_repairs(): void
    {
        Author::create(['name' => 'Corruptible Author']);

        $query = Author::where('name', 'Corruptible Author');
        $query->get();

        $base = $query->toBase();
        $base->columns = null;
        $hash = QueryHasher::fromQuery($base);
        $classKey = app('normcache')->classKey(Author::class);
        $version = app('normcache')->currentVersion(Author::class);

        Redis::connection(config('normcache.connection'))->set(
            config('normcache.key_prefix') . "query:{{$classKey}}:v{$version}:{$hash}",
            '{not-json'
        );

        Event::fake([QueryCacheMiss::class]);

        $found = Author::where('name', 'Corruptible Author')->get();

        $this->assertCount(1, $found);
        Event::assertDispatched(QueryCacheMiss::class);

        $raw = app('normcache')->getStore()->getRaw("query:{{$classKey}}:v{$version}:{$hash}");
        $repaired = $raw !== null ? json_decode($raw, true) : null;
        $this->assertSame([$found->first()->id], $repaired);
    }

    public function test_empty_query_result_warm_hit_stays_empty(): void
    {
        $first = Author::where('name', 'Missing Author')->get();
        $second = Author::where('name', 'Missing Author')->get();

        $this->assertCount(0, $first);
        $this->assertCount(0, $second);
    }

    public function test_fast_path_is_used_for_single_primary_key_lookup_with_order_by()
    {
        $author = Author::create(['name' => 'Order Author']);

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        $found = Author::where('id', $author->id)->orderBy('id')->get();

        $this->assertCount(1, $found);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_fast_path_skips_where_in_with_order_by()
    {
        Author::create(['name' => 'Order A']);
        Author::create(['name' => 'Order B']);

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        Author::whereIn('id', [1, 2])->orderBy('id')->get();

        Event::assertDispatched(QueryCacheMiss::class);
    }
}
