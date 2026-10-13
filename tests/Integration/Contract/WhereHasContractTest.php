<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Integration\Contract\Concerns\CreatesEloquentContractFixtures;
use NormCache\Tests\TestCase;

/**
 * Contract tests for cacheable relationship-existence queries.
 */
final class WhereHasContractTest extends TestCase
{
    use CreatesEloquentContractFixtures;

    public function test_simple_has_many_where_has_matches_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Author::create(['name' => 'Bob']);

        $this->contract(
            fn() => Author::whereHas('posts')->get(),
            fn() => Author::withoutCache()->whereHas('posts')->get(),
        );
    }

    public function test_where_has_with_a_safe_constraint_matches_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Published', 'author_id' => $author->id, 'published' => true]);
        Post::create(['title' => 'Draft', 'author_id' => $author->id, 'published' => false]);
        Author::create(['name' => 'Bob']);

        $this->contract(
            fn() => Author::whereHas('posts', fn($query) => $query->where('published', true))->get(),
            fn() => Author::withoutCache()->whereHas('posts', fn($query) => $query->where('published', true))->get(),
        );
    }

    public function test_or_where_has_matches_native_eloquent(): void
    {
        Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Post::create(['title' => 'Hello', 'author_id' => $bob->id]);

        $this->contract(
            fn() => Author::where('name', 'Carol')->orWhereHas('posts')->get(),
            fn() => Author::withoutCache()->where('name', 'Carol')->orWhereHas('posts')->get(),
        );
    }

    public function test_morph_many_where_has_matches_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Comment::create(['body' => 'Hi', 'commentable_type' => Author::class, 'commentable_id' => $author->id]);
        Author::create(['name' => 'Bob']);

        $this->contract(
            fn() => Author::whereHas('comments')->get(),
            fn() => Author::withoutCache()->whereHas('comments')->get(),
        );
    }

    public function test_depends_on_get(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::query()
                ->whereHas('posts', fn($q) => $q->where('published', true))
                ->dependsOn([Post::class])
                ->orderBy('name')
                ->get(),
            fn() => Author::withoutCache()
                ->whereHas('posts', fn($q) => $q->where('published', true))
                ->orderBy('name')
                ->get(),
        );
    }

    public function test_depends_on_count(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::query()
                ->whereHas('posts', fn($q) => $q->where('published', true))
                ->dependsOn([Post::class])
                ->count(),
            fn() => Author::withoutCache()
                ->whereHas('posts', fn($q) => $q->where('published', true))
                ->count(),
        );
    }

    public function test_doesnt_have_returns_correct_models(): void
    {
        $this->fixtures(); // Carol has no posts
        $this->contract(
            fn() => Author::doesntHave('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->doesntHave('posts')->orderBy('name')->get(),
        );
    }

    public function test_has_with_count_threshold_requires_declared_dependencies(): void
    {
        $this->fixtures(); // Alice has 2 posts, Bob has 1, Carol has 0
        $query = fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->has('posts', '>=', 2)
            ->when($declared, fn($query) => $query->dependsOn([Post::class]))
            ->orderBy('name')
            ->get();

        $this->bypassContract(
            fn() => $query(false),
            fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $query(false, true),
            fn() => $query(true),
        );
    }

    public function test_where_relation_returns_same_result_as_where_has(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::whereRelation('posts', 'published', true)->orderBy('name')->get(),
            fn() => Author::withoutCache()->whereRelation('posts', 'published', true)->orderBy('name')->get(),
        );
    }

    public function test_or_where_relation_combines_conditions(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::whereRelation('posts', 'title', 'A1')
                ->orWhereRelation('posts', 'title', 'B1')
                ->orderBy('name')
                ->get(),
            fn() => Author::withoutCache()
                ->whereRelation('posts', 'title', 'A1')
                ->orWhereRelation('posts', 'title', 'B1')
                ->orderBy('name')
                ->get(),
        );
    }

    public function test_where_doesnt_have_relation_with_condition(): void
    {
        $this->fixtures(); // Carol has no posts; Alice/Bob have published posts
        $this->contract(
            fn() => Author::whereDoesntHaveRelation('posts', 'published', true)->orderBy('name')->get(),
            fn() => Author::withoutCache()->whereDoesntHaveRelation('posts', 'published', true)->orderBy('name')->get(),
        );
    }

    public function test_or_where_doesnt_have_relation(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::whereRelation('posts', 'title', 'A1')
                ->orWhereDoesntHaveRelation('posts', 'published', false)
                ->orderBy('name')
                ->get(),
            fn() => Author::withoutCache()
                ->whereRelation('posts', 'title', 'A1')
                ->orWhereDoesntHaveRelation('posts', 'published', false)
                ->orderBy('name')
                ->get(),
        );
    }

    public function test_where_has_morph_filters_by_type_and_condition(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::whereHasMorph('commentable', [Author::class], fn($q) => $q->where('name', 'Alice'))->get(),
            fn() => Comment::withoutCache()->whereHasMorph('commentable', [Author::class], fn($q) => $q->where('name', 'Alice'))->get(),
        );
    }

    public function test_doesnt_have_morph_excludes_by_type(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::doesntHaveMorph('commentable', [Author::class])->orderBy('id')->get(),
            fn() => Comment::withoutCache()->doesntHaveMorph('commentable', [Author::class])->orderBy('id')->get(),
        );
    }

    public function test_where_has_morph_with_wildcard_type(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::whereHasMorph('commentable', '*')->orderBy('id')->get(),
            fn() => Comment::withoutCache()->whereHasMorph('commentable', '*')->orderBy('id')->get(),
        );
    }

    public function test_or_where_has_morph_combines_conditions(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::whereHasMorph('commentable', [Author::class])
                ->orWhereHasMorph('commentable', [Post::class])
                ->orderBy('id')
                ->get(),
            fn() => Comment::withoutCache()
                ->whereHasMorph('commentable', [Author::class])
                ->orWhereHasMorph('commentable', [Post::class])
                ->orderBy('id')
                ->get(),
        );
    }

    public function test_where_morph_relation_shorthand(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::whereMorphRelation('commentable', [Author::class], 'name', 'Alice')->orderBy('id')->get(),
            fn() => Comment::withoutCache()->whereMorphRelation('commentable', [Author::class], 'name', 'Alice')->orderBy('id')->get(),
        );
    }

    public function test_or_where_morph_relation_shorthand(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::whereMorphRelation('commentable', [Author::class], 'name', 'Alice')
                ->orWhereMorphRelation('commentable', [Post::class], 'title', 'A1')
                ->orderBy('id')
                ->get(),
            fn() => Comment::withoutCache()
                ->whereMorphRelation('commentable', [Author::class], 'name', 'Alice')
                ->orWhereMorphRelation('commentable', [Post::class], 'title', 'A1')
                ->orderBy('id')
                ->get(),
        );
    }
}
