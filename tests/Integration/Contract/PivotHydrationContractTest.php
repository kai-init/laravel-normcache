<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

/**
 * Contract tests for CachesPivotRelation::hydratePivotRelation(), which builds the
 * first pivot model the normal way and clones it for every subsequent row instead of
 * calling newExistingPivot() per row. These exercise batches large enough to hit the
 * clone path (not just the first-row template) and guard against the clone leaking
 * one row's pivot data into another.
 */
class PivotHydrationContractTest extends TestCase
{
    public function test_belongs_to_many_pivot_hydration_matches_native_across_many_rows(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tags = collect(range(1, 5))->map(fn($i) => Tag::create(['name' => "Tag{$i}"]));

        foreach ($tags as $i => $tag) {
            $author->tags()->attach($tag->id, ['notes' => "note-{$i}"]);
        }

        $query = fn() => Author::with(['tags' => fn($q) => $q->withPivot('notes')])->get();
        $native = fn() => Author::withoutCache()->with(['tags' => fn($q) => $q->withPivot('notes')])->get();

        $this->contract($query, $native);

        // Guard against the clone-per-row optimization leaking row 1's pivot data into rows 2..N.
        $warm = $query()->first()->tags->sortBy('id')->values();
        foreach ($warm as $i => $tag) {
            $this->assertSame("note-{$i}", $tag->pivot->notes);
            $this->assertTrue($tag->pivot->exists);
            $this->assertSame($author->id, $tag->pivot->author_id);
            $this->assertSame($tag->id, $tag->pivot->tag_id);
        }
    }

    public function test_morph_to_many_pivot_hydration_matches_native_across_many_rows(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tags = collect(range(1, 4))->map(fn($i) => Tag::create(['name' => "MTag{$i}"]));

        foreach ($tags as $tag) {
            $post->tags()->attach($tag->id);
        }

        $query = fn() => Post::with('tags')->get();
        $native = fn() => Post::withoutCache()->with('tags')->get();

        $this->contract($query, $native);

        $warm = $query()->first()->tags->sortBy('id')->values();
        $this->assertSame($tags->pluck('id')->sort()->values()->all(), $warm->pluck('id')->all());
        foreach ($warm as $tag) {
            $this->assertSame($post->id, $tag->pivot->taggable_id);
            $this->assertSame(Post::class, $tag->pivot->taggable_type);
        }
    }

    public function test_pivot_hydration_matches_native_on_cache_miss_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tags = collect(range(1, 3))->map(fn($i) => Tag::create(['name' => "Tag{$i}"]));

        foreach ($tags as $i => $tag) {
            $author->tags()->attach($tag->id, ['notes' => "note-{$i}"]);
        }

        // Relation calls with explicit dependencies bypass the pivot cache entirely,
        // forcing every call through the live hydratePivotRelation() path (no cache hit).
        $query = fn() => $author->tags()->dependsOn([Post::class])->withPivot('notes')->get();
        $native = fn() => $author->tags()->withoutCache()->withPivot('notes')->get();

        $this->contract($query, $native);
    }
}
