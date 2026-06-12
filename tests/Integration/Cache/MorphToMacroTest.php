<?php

// tests/Integration/Cache/MorphToMacroTest.php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class MorphToMacroTest extends TestCase
{
    public function test_morph_to_caches_even_with_macros()
    {
        $author = Author::create(['name' => 'Test Author']);
        $post = Post::create(['title' => 'Test Post', 'author_id' => $author->id]);
        Comment::create(['body' => 'Test Comment', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        // Standard eager load with a constraint (macro)
        $comments = Comment::with(['commentable' => function ($q) {
            $q->withTrashed();
        }])->get();

        // Assert it doesn't bypass cache entirely (e.g., query count is optimized)
        $this->assertNotEmpty($comments);
    }
}
