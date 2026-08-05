<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\Relations\HasOne;
use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

final class DefaultEagerAuthor extends Author
{
    protected $table = 'authors';

    protected $with = ['posts'];

    public function posts(): HasMany
    {
        return $this->hasMany(Post::class, 'author_id');
    }
}

final class AliasedPivotAuthor extends Author
{
    protected $table = 'authors';

    public function memberships(): BelongsToMany
    {
        return $this->belongsToMany(Tag::class, 'author_tag', 'author_id', 'tag_id')
            ->as('membership')
            ->withPivot('notes');
    }
}

final class OneOfManyAuthor extends Author
{
    protected $table = 'authors';

    public function oldestPost(): HasOne
    {
        return $this->hasOne(Post::class, 'author_id')->oldestOfMany();
    }
}

final class AdvancedQueryShapeContractTest extends TestCase
{
    private function fixtures(): array
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $bob = Author::create(['name' => 'Bob', 'country_id' => $country->id]);
        $carol = Author::create(['name' => 'Carol']);

        $a1 = Post::create([
            'title' => 'A1',
            'author_id' => $alice->id,
            'views' => 10,
            'published' => true,
        ]);
        $a2 = Post::create([
            'title' => 'A2',
            'author_id' => $alice->id,
            'views' => 20,
            'published' => false,
        ]);
        $b1 = Post::create([
            'title' => 'B1',
            'author_id' => $bob->id,
            'views' => 30,
            'published' => true,
        ]);

        $php = Tag::create(['name' => 'php']);
        $laravel = Tag::create(['name' => 'laravel']);
        $alice->tags()->attach($php->id, ['notes' => 'primary']);
        $alice->tags()->attach($laravel->id, ['notes' => 'secondary']);
        $bob->tags()->attach($php->id, ['notes' => 'secondary']);

        $authorComment = Comment::create([
            'body' => 'Author comment',
            'commentable_type' => Author::class,
            'commentable_id' => $alice->id,
        ]);
        $postComment = Comment::create([
            'body' => 'Post comment',
            'commentable_type' => Post::class,
            'commentable_id' => $a1->id,
        ]);

        return compact(
            'country',
            'alice',
            'bob',
            'carol',
            'a1',
            'a2',
            'b1',
            'php',
            'laravel',
            'authorComment',
            'postComment',
        );
    }

    public function test_select_sub_infers_dependencies_and_caches(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $query = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->addSelect([
                'published_posts' => Post::query()
                    ->selectRaw('count(*)')
                    ->whereColumn('posts.author_id', 'authors.id')
                    ->where('published', true),
            ])
            ->orderBy('authors.id')
            ->get();

        $this->contract(
            fn() => $query(false),
            fn() => $query(true),
            mutate: fn() => Post::create([
                'title' => 'A3',
                'author_id' => $alice->id,
                'views' => 40,
                'published' => true,
            ]),
        );
    }

    public function test_relation_subquery_uses_laravel_base_query_and_caches(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $query = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->select('authors.*')
            ->selectSub(
                $alice->posts()->selectRaw('max(views)'),
                'alice_max_views',
            )
            ->orderBy('authors.id')
            ->get();

        $this->contract(
            fn() => $query(false),
            fn() => $query(true),
            mutate: fn() => Post::create([
                'title' => 'A3',
                'author_id' => $alice->id,
                'views' => 40,
                'published' => true,
            ]),
        );
    }

    public function test_opaque_from_sub_requires_declared_dependencies(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $query = fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->fromSub(
                Author::query()->select(['id', 'name', 'country_id', 'created_at', 'updated_at']),
                'derived_authors',
            )
            ->when($declared, fn($query) => $query->dependsOn([Author::class]))
            ->select('derived_authors.*')
            ->orderBy('derived_authors.id')
            ->get();

        $this->bypassContract(
            fn() => $query(false),
            fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $query(false, true),
            fn() => $query(true),
            mutate: fn() => Author::whereKey($alice->id)->update(['name' => 'Alice Updated']),
        );
    }

    public function test_join_sub_and_left_join_sub_cache(): void
    {
        $this->fixtures();
        $publishedAuthors = fn() => Post::query()
            ->select('author_id')
            ->where('published', true)
            ->groupBy('author_id');

        $inner = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->joinSub($publishedAuthors(), 'published_posts', 'published_posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->orderBy('authors.id')
            ->get();
        $left = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->leftJoinSub($publishedAuthors(), 'published_posts', 'published_posts.author_id', '=', 'authors.id')
            ->select('authors.*')
            ->orderBy('authors.id')
            ->get();

        $this->contract(fn() => $inner(false), fn() => $inner(true));
        $this->contract(fn() => $left(false), fn() => $left(true));
    }

    public function test_cross_join_sub_requires_declared_dependencies(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $query = fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->crossJoinSub(Post::query()->selectRaw('max(views) as max_views'), 'post_stats')
            ->when($declared, fn($query) => $query->dependsOn([Post::class]))
            ->select('authors.*')
            ->addSelect('post_stats.max_views')
            ->orderBy('authors.id')
            ->get();

        $this->bypassContract(
            fn() => $query(false),
            fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $query(false, true),
            fn() => $query(true),
            mutate: fn() => Post::create([
                'title' => 'A3',
                'author_id' => $alice->id,
                'views' => 40,
                'published' => true,
            ]),
        );
    }

    public function test_predicate_subqueries_cache(): void
    {
        $this->fixtures();
        $exists = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->whereExists(function ($query): void {
                $query->selectRaw('1')
                    ->from('posts')
                    ->whereColumn('posts.author_id', 'authors.id')
                    ->where('published', true);
            })
            ->orderBy('authors.id')
            ->get();
        $notExists = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->whereNotExists(function ($query): void {
                $query->selectRaw('1')
                    ->from('posts')
                    ->whereColumn('posts.author_id', 'authors.id');
            })
            ->orderBy('authors.id')
            ->get();
        $whereIn = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->whereIn(
                'authors.id',
                Post::query()->select('author_id')->where('published', true),
            )
            ->dependsOn([Post::class])
            ->orderBy('authors.id')
            ->get();
        $scalar = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->where(
                'authors.id',
                '=',
                Author::query()->select('id')->where('name', 'Alice')->limit(1),
            )
            ->get();

        $this->contract(fn() => $exists(false), fn() => $exists(true));
        $this->contract(fn() => $notExists(false), fn() => $notExists(true));
        $this->contract(fn() => $whereIn(false), fn() => $whereIn(true));
        $this->contract(fn() => $scalar(false), fn() => $scalar(true));
    }

    public function test_order_by_subquery_requires_declared_dependencies(): void
    {
        $this->fixtures();
        $query = fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->orderBy(
                Post::query()
                    ->selectRaw('max(views)')
                    ->whereColumn('posts.author_id', 'authors.id'),
                'desc',
            )
            ->when($declared, fn($query) => $query->dependsOn([Post::class]))
            ->orderBy('authors.id')
            ->get();

        $this->bypassContract(
            fn() => $query(false),
            fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $query(false, true),
            fn() => $query(true),
            mutate: fn() => Post::query()->orderBy('id')->firstOrFail()->update(['views' => 100]),
        );
    }

    public function test_union_and_union_all_cache(): void
    {
        $this->fixtures();
        $union = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->select('authors.*')
            ->where('name', 'Alice')
            ->union(Author::query()->select('authors.*')->where('name', 'Bob'))
            ->orderBy('name')
            ->get();
        $unionAll = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->select('authors.*')
            ->where('name', 'Alice')
            ->unionAll(Author::query()->select('authors.*')->whereIn('name', ['Bob', 'Carol']))
            ->orderBy('name')
            ->get();

        $this->contract(fn() => $union(false), fn() => $union(true));
        $this->contract(fn() => $unionAll(false), fn() => $unionAll(true));
    }

    public function test_common_relationship_shorthand_queries_cache(): void
    {
        ['alice' => $alice, 'a1' => $a1, 'php' => $php] = $this->fixtures();

        $withWhereHas = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->withWhereHas('posts', fn($query) => $query->where('published', true))
            ->orderBy('authors.id')
            ->get();
        $whereBelongsTo = fn(bool $native) => ($native ? Post::withoutCache() : Post::query())
            ->whereBelongsTo($alice)
            ->orderBy('posts.id')
            ->get();
        $whereAttachedTo = fn(bool $native) => ($native ? Author::withoutCache() : Author::query())
            ->whereAttachedTo($php, 'tags')
            ->orderBy('authors.id')
            ->get();
        $whereMorphedTo = fn(bool $native) => ($native ? Comment::withoutCache() : Comment::query())
            ->whereMorphedTo('commentable', $a1)
            ->orderBy('comments.id')
            ->get();
        $whereNotMorphedTo = fn(bool $native) => ($native ? Comment::withoutCache() : Comment::query())
            ->whereNotMorphedTo('commentable', $a1)
            ->orderBy('comments.id')
            ->get();

        $this->contract(fn() => $withWhereHas(false), fn() => $withWhereHas(true));
        $this->contract(fn() => $whereBelongsTo(false), fn() => $whereBelongsTo(true));
        $this->contract(fn() => $whereAttachedTo(false), fn() => $whereAttachedTo(true));
        $this->contract(fn() => $whereMorphedTo(false), fn() => $whereMorphedTo(true));
        $this->contract(fn() => $whereNotMorphedTo(false), fn() => $whereNotMorphedTo(true));
    }

    public function test_default_eager_load_controls_and_load_morph_cache(): void
    {
        $this->fixtures();
        $withOnly = fn(bool $native) => ($native ? DefaultEagerAuthor::withoutCache() : DefaultEagerAuthor::query())
            ->withOnly('country')
            ->orderBy('authors.id')
            ->get();
        $loadMorph = fn(bool $native) => ($native ? Comment::withoutCache() : Comment::query())
            ->orderBy('comments.id')
            ->get()
            ->loadMorph('commentable', [
                Author::class => ['posts'],
                Post::class => ['author'],
            ]);
        $loadMorphCount = fn(bool $native) => ($native ? Comment::withoutCache() : Comment::query())
            ->orderBy('comments.id')
            ->get()
            ->loadMorphCount('commentable', [
                Author::class => ['posts'],
                Post::class => ['comments'],
            ]);

        $this->contract(fn() => $withOnly(false), fn() => $withOnly(true));
        $this->contract(fn() => $loadMorph(false), fn() => $loadMorph(true));
        $this->contract(fn() => $loadMorphCount(false), fn() => $loadMorphCount(true));
    }

    public function test_pivot_alias_constraints_and_oldest_of_many_cache(): void
    {
        $this->fixtures();
        $pivot = fn(bool $native) => ($native ? AliasedPivotAuthor::withoutCache() : AliasedPivotAuthor::query())
            ->with([
                'memberships' => fn($query) => $query
                    ->wherePivot('notes', 'primary')
                    ->orderByPivot('notes'),
            ])
            ->orderBy('authors.id')
            ->get();
        $oldest = fn(bool $native) => ($native ? OneOfManyAuthor::withoutCache() : OneOfManyAuthor::query())
            ->with('oldestPost')
            ->orderBy('authors.id')
            ->get();

        $this->contract(fn() => $pivot(false), fn() => $pivot(true));
        $this->contract(fn() => $oldest(false), fn() => $oldest(true));
    }

    public function test_postgres_lateral_join_requires_declared_dependencies(): void
    {
        if (DB::connection()->getDriverName() !== 'pgsql') {
            $this->markTestSkipped('Lateral join contract requires PostgreSQL.');
        }

        ['alice' => $alice] = $this->fixtures();
        $query = fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->leftJoinLateral(
                Post::query()
                    ->select(['posts.author_id', 'posts.title'])
                    ->whereColumn('posts.author_id', 'authors.id')
                    ->orderByDesc('posts.id')
                    ->limit(1),
                'latest_post',
            )
            ->when($declared, fn($query) => $query->dependsOn([Post::class]))
            ->select('authors.*')
            ->addSelect('latest_post.title as latest_title')
            ->orderBy('authors.id')
            ->get();

        $this->bypassContract(
            fn() => $query(false),
            fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $query(false, true),
            fn() => $query(true),
            mutate: fn() => Post::create([
                'title' => 'Newest',
                'author_id' => $alice->id,
                'published' => true,
            ]),
        );
    }

    public function test_postgres_table_expression_bypasses_without_declared_dependencies(): void
    {
        if (DB::connection()->getDriverName() !== 'pgsql') {
            $this->markTestSkipped('TABLE expression contract requires PostgreSQL.');
        }

        $this->fixtures();
        $query = fn() => Author::query()
            ->whereRaw('exists (table comments)')
            ->orderBy('authors.id')
            ->get();
        $native = fn() => Author::withoutCache()
            ->whereRaw('exists (table comments)')
            ->orderBy('authors.id')
            ->get();

        $this->bypassContract($query, $native, reason: 'unidentifiable_dependency');
    }
}
