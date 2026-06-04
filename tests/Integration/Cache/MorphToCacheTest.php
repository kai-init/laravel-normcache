<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedAuthor;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: morphTo() eager loads serve related models from cache when the
 * related type uses Cacheable, and fall back to Eloquent when it does not or when
 * per-type constraints are present.
 */
class MorphToCacheTest extends TestCase
{
    public function test_morph_to_uses_cache_on_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Comment::create(['body' => 'Nice', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

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

        Post::find($post->id);

        DB::enableQueryLog();
        Comment::with(['commentable' => fn($q) => $q->withTrashed()])->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $sqlTables = array_column($queries, 'query');
        $hitDb = count(array_filter($sqlTables, fn($q) => str_contains($q, '"posts"'))) > 0;
        $this->assertTrue($hitDb, 'withTrashed() macro should force DB fallback');
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

    public function test_morph_to_any_constraint_forces_db_fallback_via_macro_buffer(): void
    {
        $post = Post::create(['title' => 'Hello', 'author_id' => Author::create(['name' => 'Alice'])->id]);
        Comment::create(['body' => 'Nice', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        Comment::with('commentable')->get();

        DB::enableQueryLog();
        $comments = Comment::with(['commentable' => fn($q) => $q->select('id', 'title')])->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        // Any constraint triggers the macroBuffer, which disables the MorphTo fast-path cache.
        $postQueries = array_filter($queries, fn($q) => str_contains($q['query'], '"posts"'));
        $this->assertNotEmpty($postQueries);
        $this->assertNotNull($comments->first()->commentable);
        $this->assertSame('Hello', $comments->first()->commentable->title);
    }

    public function test_morph_to_deduplicates_ids_when_multiple_comments_share_morphable(): void
    {
        $post = Post::create(['title' => 'Shared', 'author_id' => Author::create(['name' => 'Alice'])->id]);
        Comment::create(['body' => 'A', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);
        Comment::create(['body' => 'B', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);
        Comment::create(['body' => 'C', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        Comment::with('commentable')->get();

        DB::enableQueryLog();
        $comments = Comment::with('commentable')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        foreach ($comments as $comment) {
            $this->assertSame($post->id, $comment->commentable->id);
        }

        $postQueries = array_filter($queries, fn($q) => str_contains($q['query'], '"posts"'));
        $this->assertCount(0, $postQueries, 'Three comments pointing to same post should not trigger a DB query');
    }
}
