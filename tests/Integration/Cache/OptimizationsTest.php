<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Enums\CacheStatus;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: primary-key lookups use a fast path that skips query-ID resolution;
 * model payloads are fetched in a single Lua round trip; corrupt or empty cache entries
 * degrade gracefully to a miss.
 */
class OptimizationsTest extends TestCase
{
    public function test_fast_path_is_used_for_primary_key_lookup()
    {
        $author = Author::create(['name' => 'Fast Path Author']);

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        $found = Author::where('id', $author->id)->get();

        $this->assertCount(1, $found);
        $this->assertEquals('Fast Path Author', $found->first()->name);

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

        Author::where('name', 'Lua Author')->get();

        $redis = Redis::connection(config('normcache.connection'));
        $manager = app('normcache');

        $keys = $redis->keys($manager->keys()->prefixed('query:*'));
        $this->assertNotEmpty($keys);

        $value = $redis->get($keys[0]);
        $this->assertStringStartsWith('[', $value);
        $this->assertStringEndsWith(']', $value);

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
        $hash = QueryHasher::forNormalizedQuery($query, $query->toBase());
        $result = app('normcache')->queries()->fetch(Author::class, $hash, null, [], []);

        $this->assertSame(CacheStatus::Hit, $result->status);
        $this->assertSame([(string) $author->id], $result->ids);
        $this->assertIsArray($result->models);
        $this->assertIsArray($result->models[0]);
        $this->assertSame('Payload Author', $result->models[0]['name']);
    }

    public function test_corrupt_query_cache_payload_degrades_to_miss_and_repairs(): void
    {
        Author::create(['name' => 'Corruptible Author']);

        $query = Author::where('name', 'Corruptible Author');
        $query->get();

        $hash = QueryHasher::forNormalizedQuery($query, $query->toBase());
        $classKey = app('normcache')->keys()->classKey(Author::class);
        $version = app('normcache')->currentVersion(Author::class);

        $manager = app('normcache');
        $store = $manager->store();
        $fullQueryKey = $manager->keys()->prefixed("query:{$classKey}:v{$version}:{$hash}");
        Redis::connection(config('normcache.connection'))->set(
            $fullQueryKey,
            '{not-json'
        );

        Event::fake([QueryCacheMiss::class]);

        $found = Author::where('name', 'Corruptible Author')->get();

        $this->assertCount(1, $found);
        Event::assertDispatched(QueryCacheMiss::class);

        $raw = $store->getRaw($fullQueryKey);
        $repaired = $raw !== null ? json_decode($raw, true) : null;
        $this->assertSame([(string) $found->first()->id], $repaired);
    }

    public function test_empty_query_result_warm_hit_stays_empty(): void
    {
        $first = Author::where('name', 'Missing Author')->get();
        $second = Author::where('name', 'Missing Author')->get();

        $this->assertCount(0, $first);
        $this->assertCount(0, $second);
    }

    public function test_multi_dependency_query_corrupt_payload_degrades_to_miss_and_repairs(): void
    {
        $this->setClusterMode(false);

        Author::create(['name' => 'Multi Dep Author']);

        Author::query()->dependsOn([Post::class])->get();

        $queryKey = collect($this->redisKeys('query:*'))->first();
        $this->assertNotNull($queryKey);

        Redis::connection(config('normcache.connection'))->set($queryKey, '{not-json');

        Event::fake([QueryCacheMiss::class]);

        $found = Author::query()->dependsOn([Post::class])->get();

        $this->assertCount(1, $found);
        Event::assertDispatched(QueryCacheMiss::class);

        $store = app('normcache')->store();
        $raw = $store->getRaw($queryKey);
        $repaired = $raw !== null ? json_decode($raw, true) : null;
        $this->assertSame([(string) $found->first()->id], $repaired);
    }

    public function test_large_id_list_round_trips_correctly(): void
    {
        $names = [];
        for ($i = 0; $i < 1200; $i++) {
            $names[] = ['name' => "Bulk Author {$i}"];
        }
        Author::insert($names);

        $cold = Author::orderBy('id')->get();
        $warm = Author::orderBy('id')->get();

        $this->assertCount(1200, $cold);
        $this->assertSame(
            $cold->pluck('id')->all(),
            $warm->pluck('id')->all()
        );
        $this->assertSame(
            $cold->pluck('name')->all(),
            $warm->pluck('name')->all()
        );
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

    public function test_fast_path_is_used_for_single_primary_key_lookup_with_raw_order_by(): void
    {
        $author = Author::create(['name' => 'Raw Order Author']);

        Event::fake([QueryBypassed::class, QueryCacheHit::class, QueryCacheMiss::class]);

        $found = Author::where('id', $author->id)
            ->orderByRaw('CASE WHEN id = ? THEN 0 ELSE 1 END', [$author->id])
            ->get();

        $this->assertCount(1, $found);
        Event::assertNotDispatched(QueryBypassed::class);
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

    public function test_belongs_to_remains_fast_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        // Eager load belongsTo
        $p = Post::with('author')->first();
        $this->assertTrue($p->relationLoaded('author'));

        // Verify no query for author on second load
        DB::enableQueryLog();
        Post::with('author')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        // One query for posts, none for authors (because of fast path + cache)
        $this->assertCount(1, $queries);
        $this->assertStringContainsString('from "posts"', $queries[0]['query']);
    }

    public function test_where_key_ignores_fast_path_when_extra_dependencies_exist(): void
    {
        $a1 = Author::create(['name' => 'Alice']);

        $builder = Author::whereKey($a1->id)->dependsOn([Post::class]);
        $builder->get();

        $this->assertNotEmpty($this->redisKeys('query:*'));
    }
}
