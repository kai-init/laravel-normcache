<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Contract tests for model hydration after a cached query has been resolved.
 */
class ModelHydrationContractTest extends TestCase
{
    public function test_eager_loaded_models_match_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Ivy']);
        $post = Post::create(['title' => 'WithComments', 'author_id' => $author->id]);
        Comment::create(['body' => 'Nice post', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        $this->evictModelCache(Post::class, $post->id);

        $this->contract(
            cached: fn() => Post::with('comments')->whereKey($post->id)->get(),
            native: fn() => Post::withoutCache()->with('comments')->whereKey($post->id)->get(),
        );
    }

    public function test_joined_models_with_an_explicit_root_select_match_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Lyle']);
        $post = Post::create(['title' => 'Joined', 'author_id' => $author->id]);
        $this->evictModelCache(Post::class, $post->id);

        $this->contract(
            cached: fn() => Post::query()->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.*')
                ->whereKey($post->id)
                ->get(),
            native: fn() => Post::withoutCache()->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.*')
                ->whereKey($post->id)
                ->get(),
        );
    }
}
