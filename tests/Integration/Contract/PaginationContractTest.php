<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class PaginationContractTest extends TestCase
{
    public function test_paginate(): void
    {
        $this->createAuthors(5);

        $this->contract(
            fn() => Author::orderBy('id')->paginate(2),
            fn() => Author::withoutCache()->orderBy('id')->paginate(2),
        );

        $this->contract(
            fn() => Author::orderBy('id')->paginate(2, ['*'], 'page', 2),
            fn() => Author::withoutCache()->orderBy('id')->paginate(2, ['*'], 'page', 2),
        );
    }

    public function test_paginate_empty(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->paginate(10),
            fn() => Author::withoutCache()->where('name', 'nobody')->paginate(10),
        );
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

    public function test_simple_paginate(): void
    {
        $this->createAuthors(5);

        $this->contract(
            fn() => Author::orderBy('id')->simplePaginate(2),
            fn() => Author::withoutCache()->orderBy('id')->simplePaginate(2),
        );

        $this->contract(
            fn() => Author::orderBy('id')->simplePaginate(2, ['*'], 'page', 2),
            fn() => Author::withoutCache()->orderBy('id')->simplePaginate(2, ['*'], 'page', 2),
        );
    }

    public function test_simple_paginate_invalidates_on_change(): void
    {
        $this->createAuthors(3);

        Author::orderBy('id')->simplePaginate(2); // Prime cache.

        Event::fake([QueryCacheHit::class]);
        Author::orderBy('id')->simplePaginate(2);
        Event::assertDispatched(QueryCacheHit::class);

        Author::first()->update(['name' => 'Updated Name']);

        $this->assertSame(
            'Updated Name',
            Author::orderBy('id')->simplePaginate(2)->items()[0]->name,
        );
        $this->assertSame(
            Author::withoutCache()->orderBy('id')->simplePaginate(2)->items()[0]->name,
            Author::orderBy('id')->simplePaginate(2)->items()[0]->name,
        );
    }

    public function test_cursor_paginate(): void
    {
        $this->createAuthors(5);

        $this->contract(
            fn() => Author::orderBy('id')->cursorPaginate(2),
            fn() => Author::withoutCache()->orderBy('id')->cursorPaginate(2),
        );

        $p1 = Author::withoutCache()->orderBy('id')->cursorPaginate(2);
        $cursor = $p1->nextCursor();

        $this->contract(
            fn() => Author::orderBy('id')->cursorPaginate(2, ['*'], 'cursor', $cursor),
            fn() => Author::withoutCache()->orderBy('id')->cursorPaginate(2, ['*'], 'cursor', $cursor),
        );
    }

    public function test_cursor_paginate_invalidates_on_change(): void
    {
        $this->createAuthors(3);

        Author::orderBy('id')->cursorPaginate(2); // Prime cache.

        Event::fake([QueryCacheHit::class]);
        Author::orderBy('id')->cursorPaginate(2);
        Event::assertDispatched(QueryCacheHit::class);

        Author::first()->update(['name' => 'Updated Name']);

        $this->assertSame(
            'Updated Name',
            Author::orderBy('id')->cursorPaginate(2)->items()[0]->name,
        );
        $this->assertSame(
            Author::withoutCache()->orderBy('id')->cursorPaginate(2)->items()[0]->name,
            Author::orderBy('id')->cursorPaginate(2)->items()[0]->name,
        );
    }

    public function test_complex_simple_paginate_with_dependencies(): void
    {
        $author = Author::create(['name' => 'Author']);
        $author->posts()->create(['title' => 'Post 1']);
        $author->posts()->create(['title' => 'Post 2']);

        $this->contract(
            fn() => Author::join('posts', 'authors.id', '=', 'posts.author_id')
                ->select('authors.*')
                ->dependsOn([Post::class])
                ->simplePaginate(1),
            fn() => Author::withoutCache()->join('posts', 'authors.id', '=', 'posts.author_id')
                ->select('authors.*')
                ->simplePaginate(1),
        );
        Post::first()->update(['title' => 'Changed']);
        Author::join('posts', 'authors.id', '=', 'posts.author_id')
            ->select('authors.*')
            ->dependsOn([Post::class])
            ->simplePaginate(1);
    }

    public function test_complex_cursor_paginate_with_dependencies(): void
    {
        $author = Author::create(['name' => 'Author']);
        $author->posts()->create(['title' => 'Post 1']);
        $author->posts()->create(['title' => 'Post 2']);

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
    }

    private function createAuthors(int $count): void
    {
        for ($i = 1; $i <= $count; $i++) {
            Author::create(['name' => "Author {$i}"]);
        }
    }
}
