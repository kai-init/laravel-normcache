<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

final class SubqueryDependencyTest extends TestCase
{
    private Author $author;

    protected function setUp(): void
    {
        parent::setUp();

        $this->author = Author::query()->create(['name' => 'Author']);
        Post::query()->create([
            'title' => 'First',
            'author_id' => $this->author->getKey(),
            'views' => 3,
        ]);
    }

    public function test_with_count_invalidates_on_related_insert(): void
    {
        $read = fn(): int => (int) Author::withCount('posts')
            ->whereKey($this->author->getKey())
            ->firstOrFail()
            ->posts_count;

        $this->assertSame(1, $read());
        $this->assertSame(1, $read());

        Post::query()->create(['title' => 'Second', 'author_id' => $this->author->getKey()]);

        $this->assertSame(2, $read());
    }

    public function test_constrained_with_count_invalidates_on_related_update(): void
    {
        $read = fn(): int => (int) Author::withCount([
            'posts' => fn($query) => $query->where('published', true),
        ])
            ->whereKey($this->author->getKey())
            ->firstOrFail()
            ->posts_count;

        $this->assertSame(0, $read());
        $this->assertSame(0, $read());

        Post::query()->where('author_id', $this->author->getKey())->update(['published' => true]);

        $this->assertSame(1, $read());
    }

    public function test_with_sum_invalidates_on_related_update(): void
    {
        $read = fn(): int => (int) Author::withSum('posts', 'views')
            ->whereKey($this->author->getKey())
            ->firstOrFail()
            ->posts_sum_views;

        $this->assertSame(3, $read());
        $this->assertSame(3, $read());

        Post::query()->where('author_id', $this->author->getKey())->update(['views' => 10]);

        $this->assertSame(10, $read());
    }

    public function test_pivot_write_invalidates_a_belongs_to_many_count(): void
    {
        $post = Post::query()->firstOrFail();
        $tag = Tag::query()->create(['name' => 'php']);

        $read = fn(): int => (int) Post::withCount('tags')
            ->whereKey($post->getKey())
            ->firstOrFail()
            ->tags_count;

        $this->assertSame(0, $read());
        $this->assertSame(0, $read());

        $post->tags()->attach($tag->getKey());

        $this->assertSame(1, $read());
    }

    public function test_morph_write_invalidates_a_morph_many_count(): void
    {
        $post = Post::query()->firstOrFail();

        $read = fn(): int => (int) Post::withCount('comments')
            ->whereKey($post->getKey())
            ->firstOrFail()
            ->comments_count;

        $this->assertSame(0, $read());
        $this->assertSame(0, $read());

        Comment::query()->create([
            'body' => 'First',
            'commentable_type' => Post::class,
            'commentable_id' => $post->getKey(),
        ]);

        $this->assertSame(1, $read());
    }

    public function test_paginated_aggregate_stays_correct(): void
    {
        $read = fn(): int => (int) Author::withCount('posts')
            ->orderBy('id')
            ->paginate(10)
            ->first()
            ->posts_count;

        $this->assertSame(1, $read());
        $this->assertSame(1, $read());

        Post::query()->create(['title' => 'Second', 'author_id' => $this->author->getKey()]);

        $this->assertSame(2, $read());
    }

    public function test_a_nested_subquery_mutation_is_detected_at_any_depth(): void
    {
        $read = function (): int {
            $inner = DB::table('authors');
            $subquery = DB::table('comments')->selectRaw('count(*)')->whereExists($inner);
            $query = DB::table('posts')->selectSub($subquery, 'comment_count');
            $inner->from('tags');

            return (int) $query->first()->comment_count;
        };

        $this->assertSame(0, $read());
        $this->assertSame(0, $read());

        DB::table('comments')->insert([
            'body' => 'First',
            'commentable_type' => 'post',
            'commentable_id' => Post::query()->value('id'),
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->assertSame(1, $read());
    }

    public function test_a_raw_select_subquery_requires_declared_dependencies(): void
    {
        $build = fn(bool $declared = false) => Author::withCount('posts')
            ->selectRaw('(select count(*) from comments) as comment_count')
            ->when($declared, fn($query) => $query->dependsOn(['comments']))
            ->whereKey($this->author->getKey());

        $this->bypassContract(
            fn() => $build()->firstOrFail(),
            fn() => $build()->firstOrFail(),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $build(true)->firstOrFail(),
            fn() => $build()->firstOrFail(),
            mutate: fn() => Comment::query()->create([
                'body' => 'New',
                'commentable_type' => Author::class,
                'commentable_id' => $this->author->getKey(),
            ]),
        );
    }

    public function test_a_volatile_subquery_projection_still_bypasses(): void
    {
        $build = fn() => Author::query()
            ->addSelect([
                'sampled' => Post::query()
                    ->selectRaw('random()')
                    ->whereColumn('author_id', 'authors.id')
                    ->limit(1),
            ])
            ->whereKey($this->author->getKey());

        $build()->get();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $build()->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }
}
