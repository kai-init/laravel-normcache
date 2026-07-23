<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: specific Eloquent edge cases including runtime casts,
 * select aliases, and hidden-attribute state.
 */
class EloquentEdgeCasesTest extends TestCase
{
    public function test_with_casts_runtime_casting(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post', 'author_id' => $author->id, 'views' => 10]);

        Event::fake([QueryCacheMiss::class]);
        $p1 = Post::withCasts(['views' => 'string'])->first();
        $this->assertIsString($p1->views);
        $this->assertSame('10', $p1->views);
        Event::assertDispatched(QueryCacheMiss::class);

        Event::fake([QueryCacheHit::class]);
        $p2 = Post::withCasts(['views' => 'string'])->first();
        $this->assertIsString($p2->views);
        $this->assertSame('10', $p2->views);
        Event::assertDispatched(QueryCacheHit::class);

        Event::fake([QueryCacheMiss::class]);
        $p3 = Post::withCasts(['views' => 'int'])->first();
        $this->assertIsInt($p3->views);
        $this->assertSame(10, $p3->views);
        Event::assertDispatched(QueryCacheMiss::class);
    }

    public function test_select_aliases_in_normalized_mode(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Event::fake([QueryCacheMiss::class]);
        $a1 = Author::select('id', 'name as display_name')->first();
        $this->assertSame('Alice', $a1->display_name);
        Event::assertDispatched(QueryCacheMiss::class);

        Event::fake([QueryCacheHit::class]);
        $a2 = Author::select('id', 'name as display_name')->first();
        $this->assertSame('Alice', $a2->display_name);
        Event::assertDispatched(QueryCacheHit::class);
    }

    public function test_select_aliases_in_result_mode(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $query = Post::join('authors', 'authors.id', '=', 'posts.author_id')
            ->select('posts.id', 'posts.title as headline', 'authors.name as author_name')
            ->dependsOn([Author::class]);

        Event::fake([QueryCacheMiss::class]);
        $p1 = $query->first();
        $this->assertSame('P1', $p1->headline);
        $this->assertSame('Alice', $p1->author_name);
        Event::assertDispatched(QueryCacheMiss::class);

        Event::fake([QueryCacheHit::class]);
        $p2 = $query->first();
        $this->assertSame('P1', $p2->headline);
        $this->assertSame('Alice', $p2->author_name);
        Event::assertDispatched(QueryCacheHit::class);
    }

    public function test_make_hidden_on_warm_cache_hit_hides_attribute(): void
    {
        Author::create(['name' => 'Alice']);

        $cold = Author::where('name', 'Alice')->get()->first();
        $this->assertArrayHasKey('name', $cold->toArray());

        $warm = Author::where('name', 'Alice')->get()->first()->makeHidden('name');
        $this->assertArrayNotHasKey('name', $warm->toArray(), 'makeHidden() must work on warm-cache hydrated models');
    }
}
