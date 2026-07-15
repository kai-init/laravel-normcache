<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\CustomPostCollection;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class CustomBehaviorContractTest extends TestCase
{
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

    public function test_custom_collection_is_returned_from_cache(): void
    {
        $post1 = Post::create(['title' => 'C1', 'author_id' => 1]);
        $post2 = Post::create(['title' => 'C2', 'author_id' => 1]);

        $this->contract(
            fn() => Post::whereIn('id', [$post1->id, $post2->id])->get(),
            fn() => Post::withoutCache()->whereIn('id', [$post1->id, $post2->id])->get(),
        );

        $cached = Post::whereIn('id', [$post1->id, $post2->id])->get();
        $this->assertInstanceOf(CustomPostCollection::class, $cached);
    }
}
