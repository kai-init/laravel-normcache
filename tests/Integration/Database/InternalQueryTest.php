<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class InternalQueryTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $authorId = DB::table('authors')->insertGetId(['name' => 'Author']);

        for ($i = 0; $i < 3; $i++) {
            DB::table('posts')->insertGetId([
                'title' => "Post {$i}",
                'views' => $i,
                'published' => true,
                'author_id' => $authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    public function test_internal_queries_dispatch_no_cache_events(): void
    {
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class, QueryBypassed::class]);

        $rows = Post::query()->toBase()->internal()->get();

        $this->assertCount(3, $rows);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryBypassed::class);
    }

    public function test_internal_queries_write_nothing_to_the_cache(): void
    {
        Post::query()->toBase()->internal()->get();

        $this->assertSame([], $this->cacheKeysMatching(''));
    }
}
