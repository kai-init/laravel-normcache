<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Contract tests for cacheable relationship-existence queries.
 */
class WhereHasContractTest extends TestCase
{
    public function test_simple_has_many_where_has_matches_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Author::create(['name' => 'Bob']);

        $this->contract(
            fn() => Author::whereHas('posts')->get(),
            fn() => Author::withoutCache()->whereHas('posts')->get(),
        );
    }

    public function test_where_has_with_a_safe_constraint_matches_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Published', 'author_id' => $author->id, 'published' => true]);
        Post::create(['title' => 'Draft', 'author_id' => $author->id, 'published' => false]);
        Author::create(['name' => 'Bob']);

        $this->contract(
            fn() => Author::whereHas('posts', fn($query) => $query->where('published', true))->get(),
            fn() => Author::withoutCache()->whereHas('posts', fn($query) => $query->where('published', true))->get(),
        );
    }

    public function test_or_where_has_matches_native_eloquent(): void
    {
        Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Hello', 'author_id' => $bob->id]);

        $this->contract(
            fn() => Author::where('name', 'Carol')->orWhereHas('posts')->get(),
            fn() => Author::withoutCache()->where('name', 'Carol')->orWhereHas('posts')->get(),
        );
    }

    public function test_morph_many_where_has_matches_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Comment::create(['body' => 'Hi', 'commentable_type' => Author::class, 'commentable_id' => $author->id]);
        Author::create(['name' => 'Bob']);

        $this->contract(
            fn() => Author::whereHas('comments')->get(),
            fn() => Author::withoutCache()->whereHas('comments')->get(),
        );
    }
}
