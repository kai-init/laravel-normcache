<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use NormCache\Values\CachePlanContext;

/**
 * "Simple" whereHas/has() caching — see
 * docs/superpowers/specs/2026-06-15-simple-wherehas-caching-design.md
 */
class WhereHasCachingTest extends TestCase
{
    // Classification

    public function test_simple_wherehas_infers_related_table_dependency(): void
    {
        $prepared = Author::whereHas('posts')->prepareCacheExecution();
        $plan = $prepared->builder->cachePlan($prepared->base, CachePlanContext::models());

        $this->assertTrue($plan->dependencies->safe);
        $this->assertContains('testing:posts', $plan->dependencies->tables);
    }

    // HasMany — cache routing

    public function test_simple_wherehas_hasmany_uses_result_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        Author::whereHas('posts')->get();

        $this->assertNotEmpty($this->redisKeys('result:*'));
    }

    public function test_simple_wherehas_hasmany_invalidates_on_new_related_row(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $first = Author::whereHas('posts')->get();
        $this->assertSame([], $first->pluck('id')->all());

        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $second = Author::whereHas('posts')->get();
        $this->assertSame([$author->id], $second->pluck('id')->all());
    }

    // Constraint closures

    public function test_wherehas_with_safe_constraint_invalidates_on_dependency_change(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id, 'published' => true]);

        $first = Author::whereHas('posts', fn($q) => $q->where('published', true))->get();
        $this->assertSame([$author->id], $first->pluck('id')->all());

        $post->update(['published' => false]);

        $second = Author::whereHas('posts', fn($q) => $q->where('published', true))->get();
        $this->assertSame([], $second->pluck('id')->all());
    }

    public function test_wherehas_with_unsafe_constraint_bypasses(): void
    {
        $result = Author::whereHas('posts', fn($q) => $q->whereRaw('views > 0'))->explain();

        $this->assertStringStartsWith('not cached', $result);
        $this->assertStringContainsString("can't infer cache dependency", $result);
    }

    public function test_wherehas_relation_definition_with_lock_bypasses(): void
    {
        $result = Author::whereHas('lockedPosts')->explain();

        $this->assertStringStartsWith('not cached', $result);
    }

    public function test_wherehas_relation_definition_with_without_cache_remains_cacheable(): void
    {
        $result = Author::whereHas('cacheSkippedPosts')->explain();

        $this->assertStringStartsWith('cached', $result);
    }

    public function test_wherehas_relation_on_non_cacheable_model_bypasses(): void
    {
        $result = Author::whereHas('uncachedPosts')->explain();

        $this->assertStringStartsWith('not cached', $result);
    }

    // whereDoesntHave / orWhereHas / count thresholds

    public function test_wheredoesnthave_caches_and_invalidates_on_membership_change(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $other = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Hello', 'author_id' => $other->id]);

        $first = Author::whereDoesntHave('posts')->get();
        $this->assertSame([$author->id], $first->pluck('id')->all());

        Post::create(['title' => 'New', 'author_id' => $author->id]);

        $second = Author::whereDoesntHave('posts')->get();
        $this->assertSame([], $second->pluck('id')->all());
    }

    public function test_count_threshold_has_bypasses(): void
    {
        $result = Author::has('posts', '>', 1)->explain();

        $this->assertStringStartsWith('not cached', $result);
    }

    // BelongsToMany — pivot table dependency

    public function test_wherehas_belongstomany_caches_and_invalidates_on_pivot_change(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $php = Tag::create(['name' => 'php']);
        Author::create(['name' => 'Bob']);

        $first = Author::whereHas('tags')->get();
        $this->assertSame([], $first->pluck('id')->all());

        $author->tags()->attach($php->id);

        $second = Author::whereHas('tags')->get();
        $this->assertSame([$author->id], $second->pluck('id')->all());
    }

    // HasManyThrough — through-parent dependency

    public function test_wherehas_hasmanythrough_caches_and_invalidates_on_through_parent_change(): void
    {
        $uk = Country::create(['name' => 'UK']);
        $us = Country::create(['name' => 'US']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $us->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $first = Country::whereHas('posts')->get();
        $this->assertSame([$us->id], $first->pluck('id')->all());

        $author->update(['country_id' => $uk->id]);

        $second = Country::whereHas('posts')->get();
        $this->assertSame([$uk->id], $second->pluck('id')->all());
    }

    // MorphMany allowed, MorphTo bails

    public function test_wherehas_morphto_bails(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Comment::create(['body' => 'Hi', 'commentable_type' => Author::class, 'commentable_id' => $author->id]);

        $result = Comment::whereHas('commentable')->explain();

        $this->assertStringStartsWith('not cached', $result);
        $this->assertStringContainsString("can't infer cache dependency", $result);
    }

    // Nested/dotted — bails

    public function test_wherehas_nested_dotted_bails(): void
    {
        $result = Author::whereHas('posts.comments')->explain();

        $this->assertStringStartsWith('not cached', $result);
        $this->assertStringContainsString("can't infer cache dependency", $result);
    }

    // Mixed query — non-simple predicate forces whole-query bypass

    public function test_mixed_query_with_raw_where_bypasses_despite_simple_wherehas(): void
    {
        $result = Author::whereHas('posts')->whereRaw('1 = 1')->explain();

        $this->assertStringStartsWith('not cached', $result);
    }
}
