<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Eloquent\Relations\Relation;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

/**
 * Contract tests: eager loading operations must return identical results on
 * the native path (withoutCache), cold-cache path, and warm-cache path.
 */
class RelationContractTest extends TestCase
{
    private function fixtures(): array
    {
        $country = Country::create(['name' => 'UK']);

        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $bob = Author::create(['name' => 'Bob',   'country_id' => $country->id]);
        $carol = Author::create(['name' => 'Carol']);

        $p1 = Post::create(['title' => 'A1', 'author_id' => $alice->id, 'views' => 10, 'published' => true]);
        $p2 = Post::create(['title' => 'A2', 'author_id' => $alice->id, 'views' => 20, 'published' => false]);
        $p3 = Post::create(['title' => 'B1', 'author_id' => $bob->id,   'views' => 30, 'published' => true]);

        $php = Tag::create(['name' => 'php']);
        $laravel = Tag::create(['name' => 'laravel']);

        $alice->tags()->attach([$php->id, $laravel->id]);
        $bob->tags()->attach($php->id);

        $p1->tags()->attach($php->id);

        $c1 = Comment::create(['body' => 'Great!',   'commentable_type' => Author::class, 'commentable_id' => $alice->id]);
        $c2 = Comment::create(['body' => 'Nice post', 'commentable_type' => Post::class,  'commentable_id' => $p1->id]);

        return compact('country', 'alice', 'bob', 'carol', 'p1', 'p2', 'p3', 'php', 'laravel', 'c1', 'c2');
    }

    // Eager loading

    public function test_with_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('posts')->orderBy('name')->get(),
            expectNoStrayQueries: true,
        );
    }

    public function test_with_has_one(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with('firstPost')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('firstPost')->orderBy('name')->get(),
        );
    }

    public function test_with_belongs_to(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::with('author')->orderBy('title')->get(),
            fn() => Post::withoutCache()->with('author')->orderBy('title')->get(),
        );
    }

    public function test_with_belongs_to_computed_select_matches_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::with([
                'author' => fn($query) => $query->selectRaw('id, upper(name) as upper_name'),
            ])->orderBy('title')->get(),
            fn() => Post::withoutCache()->with([
                'author' => fn($query) => $query->selectRaw('id, upper(name) as upper_name'),
            ])->orderBy('title')->get(),
        );
    }

    public function test_with_belongs_to_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with('tags')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('tags')->orderBy('name')->get(),
        );
    }

    public function test_with_morph_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with('comments')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('comments')->orderBy('name')->get(),
        );
    }

    public function test_with_morph_one(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::with('latestComment')->orderBy('title')->get(),
            fn() => Post::withoutCache()->with('latestComment')->orderBy('title')->get(),
        );
    }

    public function test_with_morph_to_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::with('tags')->orderBy('title')->get(),
            fn() => Post::withoutCache()->with('tags')->orderBy('title')->get(),
        );
    }

    public function test_with_has_many_through(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Country::with('posts')->first(),
            fn() => Country::withoutCache()->with('posts')->first(),
        );
    }

    public function test_with_has_one_through(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Country::with('firstPost')->first(),
            fn() => Country::withoutCache()->with('firstPost')->first(),
        );
    }

    public function test_with_morph_to(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Comment::with('commentable')->orderBy('id')->get(),
            fn() => Comment::withoutCache()->with('commentable')->orderBy('id')->get(),
        );
    }

    public function test_with_morph_to_alias_and_fqcn_types_in_same_collection(): void
    {
        Relation::morphMap(['post' => Post::class]);

        try {
            $alice = Author::create(['name' => 'Alice']);
            $post = Post::create(['title' => 'P', 'author_id' => $alice->id]);
            Comment::create(['body' => 'A', 'commentable_id' => $post->id, 'commentable_type' => 'post']);
            Comment::create(['body' => 'B', 'commentable_id' => $alice->id, 'commentable_type' => Author::class]);

            $this->contract(
                fn() => Comment::with('commentable')->orderBy('id')->get(),
                fn() => Comment::withoutCache()->with('commentable')->orderBy('id')->get(),
            );
        } finally {
            Relation::morphMap([], false);
        }
    }

    public function test_with_belongs_to_null_foreign_key(): void
    {
        ['carol' => $carol] = $this->fixtures(); // Carol has no country_id
        $this->contract(
            fn() => Author::where('id', $carol->id)->with('country')->first(),
            fn() => Author::withoutCache()->where('id', $carol->id)->with('country')->first(),
        );
    }

    public function test_with_constrained_eager_load(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with(['posts' => fn($q) => $q->where('published', true)])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['posts' => fn($q) => $q->where('published', true)])->orderBy('name')->get(),
        );
    }

    public function test_with_nested_eager_load(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::with('author.tags')->orderBy('title')->get(),
            fn() => Post::withoutCache()->with('author.tags')->orderBy('title')->get(),
            expectNoStrayQueries: true,
        );
    }

    public function test_with_multidimensional_array_eager_loading(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with(['posts' => ['comments']])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['posts' => ['comments']])->orderBy('name')->get(),
        );
    }

    public function test_with_colon_notation_column_selection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with(['posts:id,title,author_id'])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['posts:id,title,author_id'])->orderBy('name')->get(),
        );
    }

    public function test_with_has_many_limit_constraint(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with(['posts' => fn($q) => $q->orderBy('title')->limit(1)])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['posts' => fn($q) => $q->orderBy('title')->limit(1)])->orderBy('name')->get(),
        );
    }

    public function test_with_belongs_to_many_limit_in_eager_load(): void
    {
        $this->fixtures(); // Alice has 2 tags (php, laravel), Bob has 1
        $this->contract(
            fn() => Author::with(['tags' => fn($q) => $q->orderBy('name')->limit(1)])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['tags' => fn($q) => $q->orderBy('name')->limit(1)])->orderBy('name')->get(),
        );
    }

    public function test_with_has_many_through_limit_in_eager_load(): void
    {
        $this->fixtures(); // Country UK has 3 posts
        $this->contract(
            fn() => Country::with(['posts' => fn($q) => $q->orderBy('title')->limit(2)])->first(),
            fn() => Country::withoutCache()->with(['posts' => fn($q) => $q->orderBy('title')->limit(2)])->first(),
        );
    }

    public function test_with_has_many_with_trashed_constraint_includes_deleted(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete(); // soft-delete one of Alice's posts

        $this->contract(
            fn() => Author::with(['posts' => fn($q) => $q->withTrashed()->orderBy('title')])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['posts' => fn($q) => $q->withTrashed()->orderBy('title')])->orderBy('name')->get(),
        );
    }

    public function test_with_constrained_load_with_count_inside_closure(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with(['posts' => fn($q) => $q->withCount('comments')])->orderBy('name')->get(),
            fn() => Author::withoutCache()->with(['posts' => fn($q) => $q->withoutAggregateCache()->withCount('comments')])->orderBy('name')->get(),
        );
    }

    public function test_with_has_one_of_many_latest(): void
    {
        $this->fixtures(); // Alice: A1(views=10), A2(views=20); Bob: B1(views=30)

        $this->contract(
            fn() => Author::with('latestPost')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('latestPost')->orderBy('name')->get(),
            expectNoStrayQueries: true,
        );
    }

    public function test_with_has_one_through_of_many_latest(): void
    {
        $this->fixtures(); // UK: latest post is B1 through Bob

        $this->contract(
            fn() => Country::with('latestPost')->orderBy('name')->get(),
            fn() => Country::withoutCache()->with('latestPost')->orderBy('name')->get(),
            expectNoStrayQueries: true,
        );
    }

    public function test_with_has_one_of_many_aggregate_column(): void
    {
        $this->fixtures(); // Alice: mostViewed=A2(20), Bob: mostViewed=B1(30), Carol: null

        $this->contract(
            fn() => Author::with('mostViewedPost')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('mostViewedPost')->orderBy('name')->get(),
            expectNoStrayQueries: true,
        );
    }

    // Collection loading (load, loadMissing, loadCount, loadSum, loadMax, loadMin)

    public function test_load_on_collection_returns_same_relations(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->load('posts')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->load('posts')),
        );
    }

    public function test_load_count_on_collection_returns_same_aggregate(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadCount('posts')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadCount('posts')),
        );
    }

    public function test_load_sum_on_collection_returns_same_aggregate(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadSum('posts', 'views')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadSum('posts', 'views')),
        );
    }

    public function test_load_missing_only_loads_unloaded_relations(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadMissing('posts')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadMissing('posts')),
        );
    }

    public function test_load_missing_with_nested_dot_notation(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadMissing('posts.comments')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadMissing('posts.comments')),
        );
    }

    public function test_load_count_multiple_relations_simultaneously(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadCount(['posts', 'comments'])),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadCount(['posts', 'comments'])),
        );
    }

    public function test_load_count_excludes_soft_deleted_models(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete(); // soft-delete one of Alice's posts

        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadCount('posts')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadCount('posts')),
        );
    }

    public function test_load_sum_multiple_relations_simultaneously(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadSum('posts', 'views')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadSum('posts', 'views')),
        );
    }

    public function test_load_max_on_collection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadMax('posts', 'views')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadMax('posts', 'views')),
        );
    }

    public function test_load_min_on_collection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => tap(Author::orderBy('name')->get(), fn($c) => $c->loadMin('posts', 'views')),
            fn() => tap(Author::withoutCache()->orderBy('name')->get(), fn($c) => $c->loadMin('posts', 'views')),
        );
    }
}
