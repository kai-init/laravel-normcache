<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: primary-key lookups take the direct model path; corrupt cache
 * entries repair on read; and large model batches preserve their results.
 */
class OptimizationsTest extends TestCase
{
    public function test_fast_path_is_used_for_primary_key_lookup(): void
    {
        $author = Author::create(['name' => 'Fast Path Author']);

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        $found = Author::where('id', $author->id)->get();

        $this->assertCount(1, $found);
        $this->assertEquals('Fast Path Author', $found->first()->name);

        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_fast_path_is_used_for_where_in_primary_key(): void
    {
        $a1 = Author::create(['name' => 'A1']);
        $a2 = Author::create(['name' => 'A2']);

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        $found = Author::whereIn('id', [$a1->id, $a2->id])->get();

        $this->assertCount(2, $found);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_corrupt_query_cache_payload_degrades_to_miss_and_repairs(): void
    {
        Author::create(['name' => 'Corruptible Author']);

        $query = Author::where('name', 'Corruptible Author');
        $query->get();

        $hash = QueryHasher::forModelIndexQuery($query, $query->toBase());
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

    public function test_malformed_count_cache_payload_is_recomputed_and_repaired(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $this->assertSame(2, Author::count());

        $countKey = collect($this->redisKeys('count:*'))->first();
        $this->assertNotNull($countKey);
        $this->corruptResultCacheEntry($countKey);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $this->assertSame(2, Author::count());
        $this->assertGreaterThan(0, $queryCount, 'Malformed payload must trigger a DB recompute');

        $queryCount = 0;
        $this->assertSame(2, Author::count());
        $this->assertSame(0, $queryCount, 'Malformed entry must be repaired so the next read is a clean hit');
    }

    public function test_malformed_exists_cache_payload_is_recomputed_and_repaired(): void
    {
        Author::create(['name' => 'Alice']);

        $this->assertTrue(Author::exists());

        $scalarKey = collect($this->redisKeys('scalar:*'))->first();
        $this->assertNotNull($scalarKey);
        $this->corruptResultCacheEntry($scalarKey);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $this->assertTrue(Author::exists());
        $this->assertGreaterThan(0, $queryCount, 'Malformed payload must trigger a DB recompute');

        $queryCount = 0;
        $this->assertTrue(Author::exists());
        $this->assertSame(0, $queryCount, 'Malformed entry must be repaired so the next read is a clean hit');
    }

    public function test_malformed_scalar_cache_payload_is_recomputed_and_repaired(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id, 'views' => 10]);

        $this->assertSame(10, Post::sum('views'));

        $scalarKey = collect($this->redisKeys('scalar:*'))->first();
        $this->assertNotNull($scalarKey);
        $this->corruptResultCacheEntry($scalarKey);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $this->assertSame(10, Post::sum('views'));
        $this->assertGreaterThan(0, $queryCount, 'Malformed payload must trigger a DB recompute');

        $queryCount = 0;
        $this->assertSame(10, Post::sum('views'));
        $this->assertSame(0, $queryCount, 'Malformed entry must be repaired so the next read is a clean hit');
    }

    // Overwrites a result-cache entry with a serialized [] — the wrong shape for any scalar/count cache.
    private function corruptResultCacheEntry(string $key): void
    {
        $serialized = $this->cacheManager()->store()->serialize([]);
        Redis::connection('normcache-test')->set($key, $serialized);
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

    public function test_fast_path_is_used_for_single_primary_key_lookup_with_order_by(): void
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

    public function test_fast_path_skips_where_in_with_order_by(): void
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
