<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
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
