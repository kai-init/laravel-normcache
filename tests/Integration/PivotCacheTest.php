<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
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

    public function test_eager_load_populates_pivot_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        $this->assertNotEmpty($this->redisKeys('test:pivot:*'));
    }

    public function test_belongs_to_many_warm_hit_returns_correct_tags(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);
        $author->tags()->attach([$tag1->id, $tag2->id]);

        Author::with('tags')->get();
        $authors = Author::with('tags')->get();

        $this->assertCount(2, $authors->first()->tags);
        $this->assertEqualsCanonicalizing(
            ['Fiction', 'Drama'],
            $authors->first()->tags->pluck('name')->all()
        );
    }

    public function test_belongs_to_many_warm_hit_zero_sql(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        DB::enableQueryLog();
        Author::with('tags')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
    }

    public function test_empty_relationship_is_cached(): void
    {
        Author::create(['name' => 'Alice']);

        Author::with('tags')->get();

        $this->assertNotEmpty($this->redisKeys('test:pivot:*'));

        $authors = Author::with('tags')->get();

        $this->assertCount(0, $authors->first()->tags);
    }

    public function test_morph_to_many_warm_hit_returns_correct_tags(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);
        $post->tags()->attach($tag->id);

        Post::with('tags')->get();
        $posts = Post::with('tags')->get();

        $this->assertCount(1, $posts->first()->tags);
        $this->assertSame('Fiction', $posts->first()->tags->first()->name);
    }

    public function test_morph_to_many_warm_hit_zero_sql(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);
        $post->tags()->attach($tag->id);

        Post::with('tags')->get();

        DB::enableQueryLog();
        Post::with('tags')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
    }

    public function test_pivot_attributes_are_preserved_on_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();
        $authors = Author::with('tags')->get();

        $pivot = $authors->first()->tags->first()->pivot;
        $this->assertSame($author->id, $pivot->author_id);
        $this->assertSame($tag->id, $pivot->tag_id);
    }

    public function test_attach_after_warm_hit_returns_updated_tags(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);
        $author->tags()->attach($tag1->id);

        Author::with('tags')->get();

        $author->tags()->attach($tag2->id);

        $authors = Author::with('tags')->get();

        $this->assertCount(2, $authors->first()->tags);
    }

    public function test_multiple_parents_warm_hit(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        $fiction = Tag::create(['name' => 'Fiction']);
        $drama = Tag::create(['name' => 'Drama']);
        $alice->tags()->attach($fiction->id);
        $bob->tags()->attach($drama->id);

        Author::with('tags')->get();
        $authors = Author::with('tags')->get()->keyBy('name');

        $this->assertSame('Fiction', $authors['Alice']->tags->first()->name);
        $this->assertSame('Drama', $authors['Bob']->tags->first()->name);
    }
}
