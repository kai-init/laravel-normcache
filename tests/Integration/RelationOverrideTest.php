<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
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
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });
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
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });
        $second = Author::with('posts')->find($author->id);
        $this->assertCount(1, $second->posts);
        $this->assertSame(0, $queryCount, 'Expected cache hit — no DB queries for eager-loaded posts');

        Post::create(['title' => 'Post 2', 'author_id' => $author->id]);

        $third = Author::with('posts')->find($author->id);
        $this->assertCount(2, $third->posts);
    }

    public function test_eager_has_many_relation_warm_hit_refetches_only_evicted_child_model(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        $first = Author::with('posts')->find($author->id);
        $this->assertCount(1, $first->posts);

        $this->evictModelCache(Post::class, $post->id);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $second = Author::with('posts')->find($author->id);

        $this->assertCount(1, $second->posts);
        $this->assertSame(
            1,
            $queryCount,
            'Expected normalized cache to refetch only the evicted Post row, not the whole relation'
        );
    }

    public function test_eager_has_many_relation_uses_normalized_query_and_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id]);

        $first = Author::with('posts')->find($author->id);
        $this->assertCount(1, $first->posts);

        $this->assertNotEmpty(
            $this->redisKeys('test:query:*:posts:*'),
            'Expected simple hasMany eager load to populate the normalized query-id cache'
        );
        $this->assertNotEmpty(
            $this->redisKeys('test:model:*:posts:*'),
            'Expected simple hasMany eager load to populate the per-id model cache'
        );
    }

    public function test_has_many_calculated_projection_is_not_cached(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Post 1', 'author_id' => $author->id, 'views' => 7]);

        $first = $author->posts()
            ->select('posts.*')
            ->selectRaw('views * 2 as doubled_views')
            ->get();

        $this->assertSame(14, (int) $first->first()->doubled_views);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $second = $author->posts()
            ->select('posts.*')
            ->selectRaw('views * 2 as doubled_views')
            ->get();

        $this->assertSame(14, (int) $second->first()->doubled_views);
        $this->assertSame(
            1,
            $queryCount,
            'Calculated columns cannot be normalized into model keys, same as a top-level query — expected a live query'
        );
    }

    public function test_eager_has_many_limit_constraint_warm_hit_uses_normalized_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'B Post', 'author_id' => $author->id]);
        Post::create(['title' => 'A Post', 'author_id' => $author->id]);

        $first = Author::with(['posts' => fn($query) => $query->orderBy('title')->limit(1)])
            ->find($author->id);

        $this->assertSame(['A Post'], $first->posts->pluck('title')->all());

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });

        $second = Author::with(['posts' => fn($query) => $query->orderBy('title')->limit(1)])
            ->find($author->id);

        $this->assertSame(['A Post'], $second->posts->pluck('title')->all());
        $this->assertSame(0, $queryCount, 'Expected limited hasMany eager load to warm-hit the normalized cache');
    }

    public function test_has_many_result_payload_with_count_invalidates_when_counted_model_changes(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Post 1', 'author_id' => $author->id]);
        $post->comments()->create(['body' => 'Comment 1']);

        $query = fn() => Author::with(['posts' => fn($builder) => $builder->withCount('comments')])
            ->find($author->id);

        $first = $query();
        $this->assertSame(1, $first->posts->first()->comments_count);
        $this->assertNotEmpty($this->redisKeys('test:result:*'));

        $warm = $query();
        $this->assertSame(1, $warm->posts->first()->comments_count);

        $post->comments()->create(['body' => 'Comment 2']);

        $after = $query();
        $this->assertSame(2, $after->posts->first()->comments_count);
    }

    public function test_has_many_subquery_constraint_bypasses_result_payload(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Post 1', 'author_id' => $author->id]);
        $post->comments()->create(['body' => 'Comment 1']);

        $posts = $author->posts()
            ->whereIn('posts.id', Comment::query()
                ->select('commentable_id')
                ->where('commentable_type', Post::class))
            ->get();

        $this->assertSame(['Post 1'], $posts->pluck('title')->all());
        $this->assertEmpty($this->redisKeys('test:result:*'));
    }

    public function test_eager_morph_many_relation_is_served_from_cache_and_invalidated(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $author->comments()->create(['body' => 'Comment 1']);

        $first = Author::with('comments')->find($author->id);
        $this->assertCount(1, $first->comments);

        $queryCount = 0;
        DB::listen(function () use (&$queryCount) {
            $queryCount++;
        });
        $second = Author::with('comments')->find($author->id);
        $this->assertCount(1, $second->comments);
        $this->assertSame(0, $queryCount, 'Expected cache hit — no DB queries for eager-loaded comments');

        $author->comments()->create(['body' => 'Comment 2']);

        $third = Author::with('comments')->find($author->id);
        $this->assertCount(2, $third->comments);
    }
}
