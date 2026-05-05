<?php

namespace NormCache\Tests\Integration;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

class PivotCacheTest extends TestCase
{
    public function test_belongs_to_many_attach_invalidates_both_sides(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);

        $authorVersionBefore = NormCache::currentVersion(Author::class);
        $tagVersionBefore = NormCache::currentVersion(Tag::class);

        $author->tags()->attach($tag->id);

        $this->assertGreaterThan($authorVersionBefore, NormCache::currentVersion(Author::class));
        $this->assertGreaterThan($tagVersionBefore, NormCache::currentVersion(Tag::class));
    }

    public function test_belongs_to_many_detach_invalidates_both_sides(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $authorVersionBefore = NormCache::currentVersion(Author::class);
        $tagVersionBefore = NormCache::currentVersion(Tag::class);

        $author->tags()->detach($tag->id);

        $this->assertGreaterThan($authorVersionBefore, NormCache::currentVersion(Author::class));
        $this->assertGreaterThan($tagVersionBefore, NormCache::currentVersion(Tag::class));
    }

    public function test_belongs_to_many_sync_invalidates_once(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);

        $authorVersionBefore = NormCache::currentVersion(Author::class);

        $author->tags()->sync([$tag1->id, $tag2->id]);

        $this->assertSame(
            $authorVersionBefore + 1,
            NormCache::currentVersion(Author::class)
        );
    }

    public function test_belongs_to_many_update_existing_pivot_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $authorVersionBefore = NormCache::currentVersion(Author::class);

        $author->tags()->updateExistingPivot($tag->id, ['notes' => 'updated']);

        $this->assertGreaterThan($authorVersionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_morph_to_many_attach_invalidates_both_sides(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);

        $postVersionBefore = NormCache::currentVersion(Post::class);
        $tagVersionBefore = NormCache::currentVersion(Tag::class);

        $post->tags()->attach($tag->id);

        $this->assertGreaterThan($postVersionBefore, NormCache::currentVersion(Post::class));
        $this->assertGreaterThan($tagVersionBefore, NormCache::currentVersion(Tag::class));
    }
}
