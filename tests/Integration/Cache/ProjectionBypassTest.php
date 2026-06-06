<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

/**
 * Verifies that relation eager-load paths bypass the cache when required
 * matching keys are absent from the projected column list, and use the cache
 * when the PK is present — including qualified `table.column` forms.
 */
class ProjectionBypassTest extends TestCase
{
    // ── BelongsTo ────────────────────────────────────────────────────────────

    public function test_belongs_to_bypasses_when_owner_key_absent_from_projection(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        $native = Post::withoutCache()->with(['author' => fn($q) => $q->select('name')])->get()
            ->map(fn($p) => $p->author?->name)->all();

        $result = Post::with(['author' => fn($q) => $q->select('name')])->get()
            ->map(fn($p) => $p->author?->name)->all();

        $this->assertSame($native, $result);
        $this->assertEmpty($this->redisKeys('test:query:{authors}:*'), 'bypassed BelongsTo must not write query cache');
    }

    public function test_belongs_to_bypasses_when_owner_key_is_aliased(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        // 'authors.id as author_id' aliases the PK — normalizeProjection maps it to output key
        // 'author_id', not 'id', so the owner-key check fails and the cache path is bypassed.
        $native = Post::withoutCache()
            ->with(['author' => fn($q) => $q->select('authors.id as author_id', 'name')])
            ->get()->map(fn($p) => $p->author?->name)->all();

        $result = Post::with(['author' => fn($q) => $q->select('authors.id as author_id', 'name')])
            ->get()->map(fn($p) => $p->author?->name)->all();

        $this->assertSame($native, $result);
        $this->assertEmpty($this->redisKeys('test:query:{authors}:*'));
    }

    // ── BelongsToMany ─────────────────────────────────────────────────────────

    public function test_pivot_bypasses_when_related_pk_absent_from_projection(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $php = Tag::create(['name' => 'php']);
        $alice->tags()->attach($php->id);

        $native = Author::withoutCache()
            ->with(['tags' => fn($q) => $q->select('name')])->get()
            ->map(fn($a) => $a->tags->pluck('name')->all())->all();

        $result = Author::with(['tags' => fn($q) => $q->select('name')])->get()
            ->map(fn($a) => $a->tags->pluck('name')->all())->all();

        $this->assertSame($native, $result);
        $this->assertEmpty($this->redisKeys('test:pivot:*'), 'bypassed pivot must not write pivot cache');
    }

    public function test_pivot_uses_cache_when_qualified_related_pk_present(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $php = Tag::create(['name' => 'php']);
        $alice->tags()->attach($php->id);

        $native = Author::withoutCache()
            ->with(['tags' => fn($q) => $q->select('tags.id', 'name')])->get()
            ->map(fn($a) => $a->tags->pluck('name')->all())->all();

        $cold = Author::with(['tags' => fn($q) => $q->select('tags.id', 'name')])->get()
            ->map(fn($a) => $a->tags->pluck('name')->all())->all();

        $this->assertNotEmpty($this->redisKeys('test:pivot:*'), 'qualified PK projection must populate pivot cache');
        $this->assertSame($native, $cold);

        $warm = Author::with(['tags' => fn($q) => $q->select('tags.id', 'name')])->get()
            ->map(fn($a) => $a->tags->pluck('name')->all())->all();

        $this->assertSame($cold, $warm);
    }

    // ── HasManyThrough ───────────────────────────────────────────────────────

    public function test_through_bypasses_when_related_pk_absent_from_projection(): void
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        $native = Country::withoutCache()
            ->with(['posts' => fn($q) => $q->select('posts.title')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->all())->all();

        $result = Country::with(['posts' => fn($q) => $q->select('posts.title')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->all())->all();

        $this->assertSame($native, $result);
        $this->assertEmpty($this->redisKeys('test:through:*'), 'bypassed through must not write through cache');
    }

    public function test_through_uses_cache_when_qualified_related_pk_present(): void
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        $native = Country::withoutCache()
            ->with(['posts' => fn($q) => $q->select('posts.id', 'posts.title')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->sort()->values()->all())->all();

        $cold = Country::with(['posts' => fn($q) => $q->select('posts.id', 'posts.title')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->sort()->values()->all())->all();

        $this->assertNotEmpty($this->redisKeys('test:through:*'), 'qualified PK projection must populate through cache');
        $this->assertSame($native, $cold);

        $warm = Country::with(['posts' => fn($q) => $q->select('posts.id', 'posts.title')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->sort()->values()->all())->all();

        $this->assertSame($cold, $warm);
    }

    public function test_through_uses_cache_when_projection_is_table_wildcard(): void
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        $native = Country::withoutCache()
            ->with(['posts' => fn($q) => $q->select('posts.*')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->sort()->values()->all())->all();

        $cold = Country::with(['posts' => fn($q) => $q->select('posts.*')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->sort()->values()->all())->all();

        $this->assertNotEmpty($this->redisKeys('test:through:*'), 'table.* projection must populate through cache');
        $this->assertSame($native, $cold);

        $warm = Country::with(['posts' => fn($q) => $q->select('posts.*')])->get()
            ->map(fn($c) => $c->posts->pluck('title')->sort()->values()->all())->all();

        $this->assertSame($cold, $warm);
    }
}
