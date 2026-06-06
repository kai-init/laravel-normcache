<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class CustomBehaviorContractTest extends TestCase
{
    public function test_custom_casts_are_preserved_in_cache(): void
    {
        $post = Post::create(['title' => 'Test', 'author_id' => 1, 'published' => true, 'metadata' => ['foo' => 'bar']]);

        $this->contract(
            fn() => Post::where('id', $post->id)->get(),
            fn() => Post::withoutCache()->where('id', $post->id)->get(),
        );
    }

    public function test_hidden_and_appends_visibility_is_respected(): void
    {
        $post = Post::create(['title' => 'Secret', 'author_id' => 1]);

        $this->contract(
            function () use ($post) {
                $p = Post::where('id', $post->id)->first();
                $p->makeHidden('title');
                $p->append('calculated_field');

                return $p->toArray();
            },
            function () use ($post) {
                $p = Post::withoutCache()->where('id', $post->id)->first();
                $p->makeHidden('title');
                $p->append('calculated_field');

                return $p->toArray();
            }
        );
    }
}
