<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedAuthor;
use NormCache\Tests\TestCase;

class MorphToCacheTest extends TestCase
{
    public function test_morph_to_eager_load_returns_correct_models(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $commentOnPost = Comment::create(['body' => 'Nice post', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);
        $commentOnAuthor = Comment::create(['body' => 'Nice author', 'commentable_id' => $author->id, 'commentable_type' => Author::class]);

        $comments = Comment::with('commentable')->whereIn('id', [$commentOnPost->id, $commentOnAuthor->id])->get();

        $byId = $comments->keyBy('id');
        $this->assertInstanceOf(Post::class, $byId[$commentOnPost->id]->commentable);
        $this->assertInstanceOf(Author::class, $byId[$commentOnAuthor->id]->commentable);
        $this->assertSame($post->id, $byId[$commentOnPost->id]->commentable->id);
        $this->assertSame($author->id, $byId[$commentOnAuthor->id]->commentable->id);
    }

    public function test_morph_to_uses_cache_on_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Comment::create(['body' => 'Nice', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        // Warm the model caches
        Comment::with('commentable')->get();

        DB::enableQueryLog();
        Comment::with('commentable')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $sqlTables = array_column($queries, 'query');
        $this->assertNotContains(true, array_map(fn($q) => str_contains($q, '"posts"') || str_contains($q, '"authors"'), $sqlTables),
            'MorphTo eager load should serve from cache after warm-up, but hit DB for related models');
    }

    public function test_morph_to_falls_back_when_related_type_is_not_cacheable(): void
    {
        $uncachedAuthor = UncachedAuthor::create(['name' => 'Bob']);
        Comment::create(['body' => 'Hi', 'commentable_id' => $uncachedAuthor->id, 'commentable_type' => UncachedAuthor::class]);

        DB::enableQueryLog();
        $comments = Comment::with('commentable')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotNull($comments->first()->commentable);
        $sqlCount = count(array_filter($queries, fn($q) => str_contains($q['query'], '"authors"')));
        $this->assertGreaterThan(0, $sqlCount, 'Non-cacheable type should fall back to DB');
    }

    public function test_morph_to_falls_back_when_macro_buffer_is_set(): void
    {
        $post = Post::create(['title' => 'Hello', 'author_id' => Author::create(['name' => 'Alice'])->id]);
        Comment::create(['body' => 'Hi', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        // Warm model cache
        Post::find($post->id);

        DB::enableQueryLog();
        Comment::with(['commentable' => fn($q) => $q->withTrashed()])->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $sqlTables = array_column($queries, 'query');
        $hitDb = count(array_filter($sqlTables, fn($q) => str_contains($q, '"posts"'))) > 0;
        $this->assertTrue($hitDb, 'withTrashed() macro should force DB fallback');
    }

    public function test_morph_to_with_morphable_eager_loads_loads_nested_relation(): void
    {
        $country = Country::create(['name' => 'AU']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Comment::create(['body' => 'Nice', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        // Warm caches
        Post::with('author')->get();
        Comment::with('commentable')->get();

        $comments = Comment::with(['commentable' => fn($q) => $q->morphWith([
            Post::class => ['author'],
        ])])->get();

        $this->assertInstanceOf(Author::class, $comments->first()->commentable->author);
    }

    public function test_morph_to_falls_back_when_per_type_constraint_set(): void
    {
        $post = Post::create(['title' => 'Hello', 'published' => true, 'author_id' => Author::create(['name' => 'Alice'])->id]);
        Comment::create(['body' => 'Hi', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        DB::enableQueryLog();
        $comments = Comment::with(['commentable' => fn($q) => $q->constrain([
            Post::class => fn($q) => $q->where('published', true),
        ])])->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $hitDb = count(array_filter($queries, fn($q) => str_contains($q['query'], '"posts"'))) > 0;
        $this->assertTrue($hitDb, 'constrain() should force DB fallback for that type');
        $this->assertInstanceOf(Post::class, $comments->first()->commentable);
    }

    public function test_morph_to_falls_back_when_morph_with_count_set(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Comment::create(['body' => 'A', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);
        Comment::create(['body' => 'B', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        $comments = Comment::where('commentable_type', Post::class)
            ->with(['commentable' => fn($q) => $q->morphWithCount([Post::class => ['comments']])])
            ->get();

        $this->assertNotNull($comments->first()->commentable);
        $this->assertSame(2, $comments->first()->commentable->comments_count);
    }

    public function test_morph_to_deduplicates_ids_when_multiple_comments_share_morphable(): void
    {
        $post = Post::create(['title' => 'Shared', 'author_id' => Author::create(['name' => 'Alice'])->id]);
        Comment::create(['body' => 'A', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);
        Comment::create(['body' => 'B', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);
        Comment::create(['body' => 'C', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        // Warm
        Comment::with('commentable')->get();

        DB::enableQueryLog();
        $comments = Comment::with('commentable')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        // All three comments should resolve to the same post
        foreach ($comments as $comment) {
            $this->assertSame($post->id, $comment->commentable->id);
        }

        // No DB hit for posts (all from model cache)
        $postQueries = array_filter($queries, fn($q) => str_contains($q['query'], '"posts"'));
        $this->assertCount(0, $postQueries, 'Three comments pointing to same post should not trigger a DB query');
    }
}
