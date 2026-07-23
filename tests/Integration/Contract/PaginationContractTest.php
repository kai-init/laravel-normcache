<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral and contract tests: paginate(), simplePaginate(), and cursorPaginate()
 * correctly utilize the result cache, handle multi-page navigation/cursors,
 * and respect invalidation while maintaining exact parity with native Eloquent.
 */
class PaginationContractTest extends TestCase
{
    // Standard paginate()

    public function test_paginate_contract(): void
    {
        $this->createAuthors(5);

        // Page 1
        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::orderBy('id')->paginate(2),
            fn() => Author::withoutCache()->orderBy('id')->paginate(2),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);

        // Page 2
        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::orderBy('id')->paginate(2, ['*'], 'page', 2),
            fn() => Author::withoutCache()->orderBy('id')->paginate(2, ['*'], 'page', 2),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
    }

    public function test_paginate_empty(): void
    {
        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::where('name', 'nobody')->paginate(10),
            fn() => Author::withoutCache()->where('name', 'nobody')->paginate(10),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
    }

    public function test_paginate_with_column_selection(): void
    {
        $this->createAuthors(5);
        $this->contract(
            fn() => Author::orderBy('id')->paginate(10, ['id', 'name']),
            fn() => Author::withoutCache()->orderBy('id')->paginate(10, ['id', 'name']),
        );
    }

    public function test_paginate_with_distinct_returns_correct_total(): void
    {
        $this->createAuthors(5);
        $this->contract(
            fn() => Author::distinct()->orderBy('id')->paginate(2),
            fn() => Author::withoutCache()->distinct()->orderBy('id')->paginate(2),
        );
    }

    // simplePaginate()

    public function test_simple_paginate_contract(): void
    {
        $this->createAuthors(5);

        // Page 1
        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::orderBy('id')->simplePaginate(2),
            fn() => Author::withoutCache()->orderBy('id')->simplePaginate(2),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);

        // Page 2
        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::orderBy('id')->simplePaginate(2, ['*'], 'page', 2),
            fn() => Author::withoutCache()->orderBy('id')->simplePaginate(2, ['*'], 'page', 2),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
    }

    // cursorPaginate()

    public function test_cursor_paginate_contract(): void
    {
        $this->createAuthors(5);

        // Page 1
        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::orderBy('id')->cursorPaginate(2),
            fn() => Author::withoutCache()->orderBy('id')->cursorPaginate(2),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);

        // Next page via cursor
        $p1 = Author::withoutCache()->orderBy('id')->cursorPaginate(2);
        $cursor = $p1->nextCursor();

        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::orderBy('id')->cursorPaginate(2, ['*'], 'cursor', $cursor),
            fn() => Author::withoutCache()->orderBy('id')->cursorPaginate(2, ['*'], 'cursor', $cursor),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
    }

    // Complex / Dependencies

    public function test_complex_simple_paginate_with_dependencies(): void
    {
        $author = Author::create(['name' => 'Author']);
        $author->posts()->create(['title' => 'Post 1']);
        $author->posts()->create(['title' => 'Post 2']);

        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::join('posts', 'authors.id', '=', 'posts.author_id')
                ->select('authors.*')
                ->dependsOn([Post::class])
                ->simplePaginate(1),
            fn() => Author::withoutCache()->join('posts', 'authors.id', '=', 'posts.author_id')
                ->select('authors.*')
                ->simplePaginate(1),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);

        Post::first()->update(['title' => 'Changed']);
        Event::fake([QueryCacheMiss::class]);
        Author::join('posts', 'authors.id', '=', 'posts.author_id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->simplePaginate(1);
        Event::assertDispatched(QueryCacheMiss::class);
    }

    public function test_complex_cursor_paginate_with_dependencies(): void
    {
        $author = Author::create(['name' => 'Author']);
        $author->posts()->create(['title' => 'Post 1']);
        $author->posts()->create(['title' => 'Post 2']);

        Event::fake([QueryCacheMiss::class, QueryCacheHit::class]);
        $this->contract(
            fn() => Author::join('posts', 'authors.id', '=', 'posts.author_id')
                ->select('authors.*')
                ->orderBy('authors.id')
                ->dependsOn([Post::class])
                ->cursorPaginate(1),
            fn() => Author::withoutCache()->join('posts', 'authors.id', '=', 'posts.author_id')
                ->select('authors.*')
                ->orderBy('authors.id')
                ->cursorPaginate(1),
        );
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertDispatched(QueryCacheHit::class);
    }

    private function createAuthors(int $count): void
    {
        for ($i = 1; $i <= $count; $i++) {
            Author::create(['name' => "Author {$i}"]);
        }
    }
}
