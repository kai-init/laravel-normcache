<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Carbon;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Contract tests: result-cache mode (dependsOn) must apply casts and return
 * identical values on cold and warm hits, with one documented exception.
 */
class ResultCacheContractTest extends TestCase
{
    private function author(): Author
    {
        return Author::create(['name' => 'Alice']);
    }

    public function test_boolean_cast_applied_on_result_cache_hit(): void
    {
        $author = $this->author();
        Post::create(['title' => 'T', 'published' => true, 'author_id' => $author->id]);

        $this->contract(
            fn() => Post::dependsOn([Author::class])->get()->first(),
            fn() => Post::withoutCache()->where('author_id', $author->id)->get()->first(),
        );

        Post::dependsOn([Author::class])->get();
        $warm = Post::dependsOn([Author::class])->get()->first();

        $this->assertIsBool($warm->published);
        $this->assertTrue($warm->published);
    }

    public function test_array_cast_applied_on_result_cache_hit(): void
    {
        $author = $this->author();
        Post::create(['title' => 'T', 'metadata' => ['x' => 1], 'author_id' => $author->id]);

        $this->contract(
            fn() => Post::dependsOn([Author::class])->get()->first(),
            fn() => Post::withoutCache()->where('author_id', $author->id)->get()->first(),
        );

        Post::dependsOn([Author::class])->get();
        $warm = Post::dependsOn([Author::class])->get()->first();

        $this->assertIsArray($warm->metadata);
        $this->assertSame(['x' => 1], $warm->metadata);
    }

    public function test_date_cast_returns_carbon_on_result_cache_hit(): void
    {
        $author = $this->author();
        Post::create(['title' => 'T', 'author_id' => $author->id]);

        $this->contract(
            fn() => Post::dependsOn([Author::class])->get()->first(),
            fn() => Post::withoutCache()->where('author_id', $author->id)->get()->first(),
        );

        Post::dependsOn([Author::class])->get();
        $warm = Post::dependsOn([Author::class])->get()->first();

        $this->assertInstanceOf(Carbon::class, $warm->created_at);
    }

    public function test_class_defined_casts_always_apply_on_warm_hit(): void
    {
        $author = $this->author();
        Post::create(['title' => 'T', 'views' => 42, 'published' => true, 'author_id' => $author->id]);

        $this->contract(
            fn() => Post::dependsOn([Author::class])->get()->first(),
            fn() => Post::withoutCache()->where('author_id', $author->id)->get()->first(),
        );

        Post::dependsOn([Author::class])->get();
        $warm = Post::dependsOn([Author::class])->get()->first();

        $this->assertIsBool($warm->published);
        $this->assertIsInt($warm->views);
    }

    public function test_with_casts_not_applied_on_result_cache_warm_hit(): void
    {
        // withCasts() adds casts to the builder's model; warm hits use a
        // prototype (new Post()) with no knowledge of those temporary casts.
        // This intentionally documents that warm != native for withCasts().
        $author = $this->author();
        Post::create(['title' => 'T', 'views' => 42, 'author_id' => $author->id]);

        Post::withCasts(['views' => 'string'])->dependsOn([Author::class])->get();
        $warm = Post::withCasts(['views' => 'string'])->dependsOn([Author::class])->get()->first();

        $this->assertIsInt($warm->views);
    }

    public function test_add_select_column_accessible_on_result_cache_hit(): void
    {
        $author = $this->author();
        Post::create(['title' => 'Hello', 'views' => 7, 'author_id' => $author->id]);

        $this->contract(
            fn() => Post::select('id')->addSelect('title')->dependsOn([Author::class])->get()->first(),
            fn() => Post::withoutCache()->select('id')->addSelect('title')->where('author_id', $author->id)->get()->first(),
        );

        Post::select('id')->addSelect('title')->dependsOn([Author::class])->get();
        $warm = Post::select('id')->addSelect('title')->dependsOn([Author::class])->get()->first();

        $this->assertSame('Hello', $warm->title);
        $this->assertNull($warm->getRawOriginal('views'));
    }

    public function test_select_raw_alias_accessible_on_result_cache_hit(): void
    {
        $author = $this->author();
        Post::create(['title' => 'T', 'views' => 10, 'author_id' => $author->id]);
        Post::create(['title' => 'T', 'views' => 20, 'author_id' => $author->id]);

        Post::selectRaw('MAX(views) as max_views')->dependsOn([Author::class])->get();
        $warm = Post::selectRaw('MAX(views) as max_views')->dependsOn([Author::class])->get()->first();

        $this->assertSame(20, (int) $warm->max_views);
    }
}
