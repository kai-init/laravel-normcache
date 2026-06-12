<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class RelationOverrideTest extends TestCase
{
    public function test_lazy_has_many_relation_is_served_from_cache_and_invalidated(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        $first = Author::find($author->id);
        $this->assertCount(1, $first->posts);

        $queryCount = 0;
        DB::listen(fn() => $queryCount++);
        $second = Author::find($author->id);
        $this->assertCount(1, $second->posts);
        $this->assertSame(0, $queryCount, 'Expected cache hit — no DB queries for lazy posts relation');

        Post::create(['title' => 'Post 2', 'author_id' => $author->id]);

        $third = Author::find($author->id);
        $this->assertCount(2, $third->posts);
    }

    public function test_eager_has_many_relation_is_served_from_cache_and_invalidated(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        $first = Author::with('posts')->find($author->id);
        $this->assertCount(1, $first->posts);

        $queryCount = 0;
        DB::listen(fn() => $queryCount++);
        $second = Author::with('posts')->find($author->id);
        $this->assertCount(1, $second->posts);
        $this->assertSame(0, $queryCount, 'Expected cache hit — no DB queries for eager-loaded posts');

        Post::create(['title' => 'Post 2', 'author_id' => $author->id]);

        $third = Author::with('posts')->find($author->id);
        $this->assertCount(2, $third->posts);
    }

    public function test_eager_morph_many_relation_is_served_from_cache_and_invalidated(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $author->comments()->create(['body' => 'Comment 1']);

        $first = Author::with('comments')->find($author->id);
        $this->assertCount(1, $first->comments);

        $queryCount = 0;
        DB::listen(fn() => $queryCount++);
        $second = Author::with('comments')->find($author->id);
        $this->assertCount(1, $second->comments);
        $this->assertSame(0, $queryCount, 'Expected cache hit — no DB queries for eager-loaded comments');

        $author->comments()->create(['body' => 'Comment 2']);

        $third = Author::with('comments')->find($author->id);
        $this->assertCount(2, $third->comments);
    }
}
