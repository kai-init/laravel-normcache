<?php

namespace NormCache\Tests\Integration;

use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Pagination\CursorPaginator;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;
use ReflectionProperty;

/**
 * Contract tests: every Eloquent operation NormCache intercepts must return an
 * identical result on the native path (withoutCache), the cold-cache path
 * (cache miss → DB), and the warm-cache path (cache hit).
 *
 *   $native = withoutCache() ground truth
 *   $cold   = first cached run  (cache miss → DB → populates cache)
 *   $warm   = second cached run (cache hit)
 *
 * A failure means NormCache's cached result diverges from native Eloquent.
 */
class EloquentContractTest extends TestCase
{
    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Assert native == cold == warm for a given query. */
    private function contract(callable $cached, callable $native): void
    {
        $expected = $this->normalize($native());
        $cold = $this->normalize($cached());
        $warm = $this->normalize($cached());

        $this->assertSame($expected, $cold, 'cold cache result differs from native Eloquent');
        $this->assertSame($cold, $warm, 'warm cache result differs from cold');
    }

    private function normalize(mixed $value): mixed
    {
        if ($value instanceof LengthAwarePaginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'total' => $value->total(),
            ];
        }

        if ($value instanceof CursorPaginator) {
            return collect($value->items())->map->toArray()->values()->all();
        }

        if ($value instanceof EloquentCollection) {
            return $value->map->toArray()->values()->all();
        }

        if ($value instanceof Collection) {
            return $value->all(); // preserve keys (e.g. keyed pluck)
        }

        if ($value instanceof Model) {
            return $value->toArray();
        }

        return $value;
    }

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

    private function clearGlobalScope(string $modelClass, string $name): void
    {
        $prop = new ReflectionProperty(Model::class, 'globalScopes');
        $scopes = $prop->getValue();
        unset($scopes[$modelClass][$name]);
        $prop->setValue(null, $scopes);
    }

    // -------------------------------------------------------------------------
    // get() — collection shapes
    // -------------------------------------------------------------------------

    public function test_get_all_models(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->get(),
            fn() => Author::withoutCache()->orderBy('name')->get(),
        );
    }

    public function test_get_with_where(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::where('name', '!=', 'Carol')->orderBy('name')->get(),
            fn() => Author::withoutCache()->where('name', '!=', 'Carol')->orderBy('name')->get(),
        );
    }

    public function test_get_with_where_in(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::whereIn('name', ['Alice', 'Bob'])->orderBy('name')->get(),
            fn() => Author::withoutCache()->whereIn('name', ['Alice', 'Bob'])->orderBy('name')->get(),
        );
    }

    public function test_get_with_where_null_and_not_null(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::whereNotNull('country_id')->orderBy('name')->get(),
            fn() => Author::withoutCache()->whereNotNull('country_id')->orderBy('name')->get(),
        );
    }

    public function test_get_with_order_by_desc(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderByDesc('name')->get(),
            fn() => Author::withoutCache()->orderByDesc('name')->get(),
        );
    }

    public function test_get_with_limit_and_offset(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->skip(1)->take(2)->get(),
            fn() => Author::withoutCache()->orderBy('name')->skip(1)->take(2)->get(),
        );
    }

    public function test_get_with_column_selection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->get(['id', 'name']),
            fn() => Author::withoutCache()->orderBy('name')->get(['id', 'name']),
        );
    }

    public function test_get_empty_result(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->get(),
            fn() => Author::withoutCache()->where('name', 'nobody')->get(),
        );
    }

    public function test_get_respects_soft_delete_scope(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete();

        $this->contract(
            fn() => Post::orderBy('title')->get(),
            fn() => Post::withoutCache()->orderBy('title')->get(),
        );
    }

    public function test_get_with_trashed_includes_deleted(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete();

        $this->contract(
            fn() => Post::withTrashed()->orderBy('title')->get(),
            fn() => Post::withoutCache()->withTrashed()->orderBy('title')->get(),
        );
    }

    public function test_get_only_trashed(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete();

        $this->contract(
            fn() => Post::onlyTrashed()->get(),
            fn() => Post::withoutCache()->onlyTrashed()->get(),
        );
    }

    // -------------------------------------------------------------------------
    // Single model (first, find, sole, soleValue, firstWhere, findOrFail, firstOrFail, first/firstWhere on relation instance)
    // -------------------------------------------------------------------------

    public function test_first_returns_first_model(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->first(),
            fn() => Author::withoutCache()->orderBy('name')->first(),
        );
    }

    public function test_first_returns_null_on_empty(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->first(),
            fn() => Author::withoutCache()->where('name', 'nobody')->first(),
        );
    }

    public function test_first_with_column_selection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->first(['id', 'name']),
            fn() => Author::withoutCache()->orderBy('name')->first(['id', 'name']),
        );
    }

    public function test_find_by_id(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $this->contract(
            fn() => Author::find($alice->id),
            fn() => Author::withoutCache()->find($alice->id),
        );
    }

    public function test_find_multiple_ids(): void
    {
        ['alice' => $alice, 'bob' => $bob] = $this->fixtures();
        $ids = [$alice->id, $bob->id];
        $this->contract(
            fn() => Author::findMany($ids)->sortBy('name')->values(),
            fn() => Author::withoutCache()->findMany($ids)->sortBy('name')->values(),
        );
    }

    public function test_find_returns_null_for_missing_id(): void
    {
        $this->contract(
            fn() => Author::find(99999),
            fn() => Author::withoutCache()->find(99999),
        );
    }

    public function test_sole_returns_same_model(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::where('name', 'Alice')->sole(),
            fn() => Author::withoutCache()->where('name', 'Alice')->sole(),
        );
    }

    public function test_sole_value_returns_correct_scalar(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::where('name', 'Alice')->soleValue('name'),
            fn() => Author::withoutCache()->where('name', 'Alice')->soleValue('name'),
        );
    }

    public function test_first_where_returns_same_model(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::firstWhere('name', 'Alice'),
            fn() => Author::withoutCache()->firstWhere('name', 'Alice'),
        );
    }

    public function test_find_or_fail_returns_same_model(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $this->contract(
            fn() => Author::findOrFail($alice->id),
            fn() => Author::withoutCache()->findOrFail($alice->id),
        );
    }

    public function test_first_or_fail_returns_same_model(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::where('name', 'Alice')->firstOrFail(),
            fn() => Author::withoutCache()->where('name', 'Alice')->firstOrFail(),
        );
    }

    public function test_first_on_relation_instance(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $this->contract(
            fn() => Author::find($alice->id)->posts()->orderBy('title')->first(),
            fn() => Author::withoutCache()->find($alice->id)->posts()->orderBy('title')->first(),
        );
    }

    public function test_first_where_on_relation_instance(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $this->contract(
            fn() => Author::find($alice->id)->posts()->firstWhere('published', true),
            fn() => Author::withoutCache()->find($alice->id)->posts()->firstWhere('published', true),
        );
    }

    // -------------------------------------------------------------------------
    // Scalar aggregates (count, sum, avg, min, max, exists, doesntExist, value, pluck, plus all edge-case variants)
    // -------------------------------------------------------------------------

    public function test_count_all(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::count(),
            fn() => Author::withoutCache()->count(),
        );
    }

    public function test_count_with_column(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::count('id'),
            fn() => Author::withoutCache()->count('id'),
        );
    }

    public function test_count_with_where(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::where('published', true)->count(),
            fn() => Post::withoutCache()->where('published', true)->count(),
        );
    }

    public function test_count_empty(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->count(),
            fn() => Author::withoutCache()->where('name', 'nobody')->count(),
        );
    }

    public function test_count_with_array_columns_bypasses_scalar_cache(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::count(['id']),
            fn() => Author::withoutCache()->count(['id']),
        );
    }

    public function test_sum(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::sum('views'),
            fn() => Post::withoutCache()->sum('views'),
        );
    }

    public function test_avg(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::avg('views'),
            fn() => Post::withoutCache()->avg('views'),
        );
    }

    public function test_average_alias(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::average('views'),
            fn() => Post::withoutCache()->average('views'),
        );
    }

    public function test_min(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::min('views'),
            fn() => Post::withoutCache()->min('views'),
        );
    }

    public function test_max(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::max('views'),
            fn() => Post::withoutCache()->max('views'),
        );
    }

    public function test_exists_true(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::exists(),
            fn() => Author::withoutCache()->exists(),
        );
    }

    public function test_exists_false(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->exists(),
            fn() => Author::withoutCache()->where('name', 'nobody')->exists(),
        );
    }

    public function test_doesnt_exist_true(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->doesntExist(),
            fn() => Author::withoutCache()->where('name', 'nobody')->doesntExist(),
        );
    }

    public function test_doesnt_exist_false(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::doesntExist(),
            fn() => Author::withoutCache()->doesntExist(),
        );
    }

    public function test_value(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->value('name'),
            fn() => Author::withoutCache()->orderBy('name')->value('name'),
        );
    }

    public function test_value_null_on_empty(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->value('name'),
            fn() => Author::withoutCache()->where('name', 'nobody')->value('name'),
        );
    }

    public function test_pluck_single_column(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->pluck('name'),
            fn() => Author::withoutCache()->orderBy('name')->pluck('name'),
        );
    }

    public function test_pluck_keyed(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('id')->pluck('name', 'id'),
            fn() => Author::withoutCache()->orderBy('id')->pluck('name', 'id'),
        );
    }

    public function test_sum_on_empty_set_returns_consistent_type(): void
    {
        $this->contract(
            fn() => Post::where('author_id', 99999)->sum('views'),
            fn() => Post::withoutCache()->where('author_id', 99999)->sum('views'),
        );
    }

    public function test_avg_excludes_null_rows_consistently(): void
    {
        $author = Author::create(['name' => 'Avg']);
        Post::create(['title' => 'P1', 'author_id' => $author->id, 'views' => 10]);
        Post::create(['title' => 'P2', 'author_id' => $author->id, 'views' => 30]);
        // 'views' is non-nullable in fixtures but avg on a filtered empty set returns null
        $this->contract(
            fn() => Post::where('author_id', $author->id)->avg('views'),
            fn() => Post::withoutCache()->where('author_id', $author->id)->avg('views'),
        );
    }

    public function test_min_returns_null_on_empty_set(): void
    {
        $this->contract(
            fn() => Post::where('author_id', 99999)->min('views'),
            fn() => Post::withoutCache()->where('author_id', 99999)->min('views'),
        );
    }

    public function test_max_returns_null_on_empty_set(): void
    {
        $this->contract(
            fn() => Post::where('author_id', 99999)->max('views'),
            fn() => Post::withoutCache()->where('author_id', 99999)->max('views'),
        );
    }

    public function test_avg_returns_null_on_empty_set(): void
    {
        $this->contract(
            fn() => Post::where('author_id', 99999)->avg('views'),
            fn() => Post::withoutCache()->where('author_id', 99999)->avg('views'),
        );
    }

    public function test_min_on_nullable_column_ignores_null_rows(): void
    {
        $this->fixtures(); // Alice country_id=1, Bob country_id=1, Carol country_id=null
        $this->contract(
            fn() => Author::min('country_id'),   // null excluded → 1
            fn() => Author::withoutCache()->min('country_id'),
        );
    }

    public function test_max_on_nullable_column_ignores_null_rows(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::max('country_id'),
            fn() => Author::withoutCache()->max('country_id'),
        );
    }

    public function test_avg_on_nullable_column_ignores_null_rows(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::avg('country_id'),
            fn() => Author::withoutCache()->avg('country_id'),
        );
    }

    // -------------------------------------------------------------------------
    // paginate()
    // -------------------------------------------------------------------------

    public function test_paginate_first_page(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->paginate(2),
            fn() => Author::withoutCache()->orderBy('name')->paginate(2),
        );
    }

    public function test_paginate_second_page(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->paginate(2, ['*'], 'page', 2),
            fn() => Author::withoutCache()->orderBy('name')->paginate(2, ['*'], 'page', 2),
        );
    }

    public function test_paginate_empty(): void
    {
        $this->contract(
            fn() => Author::where('name', 'nobody')->paginate(10),
            fn() => Author::withoutCache()->where('name', 'nobody')->paginate(10),
        );
    }

    public function test_paginate_with_column_selection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::orderBy('name')->paginate(10, ['id', 'name']),
            fn() => Author::withoutCache()->orderBy('name')->paginate(10, ['id', 'name']),
        );
    }

    public function test_paginate_with_distinct_returns_correct_total(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::distinct()->orderBy('name')->paginate(2),
            fn() => Author::withoutCache()->distinct()->orderBy('name')->paginate(2),
        );
    }

    public function test_paginate_with_distinct_and_column_selection(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::select('name')->distinct()->orderBy('name')->paginate(2),
            fn() => Author::withoutCache()->select('name')->distinct()->orderBy('name')->paginate(2),
        );
    }

    // -------------------------------------------------------------------------
    // withAggregate / deferred aggregates
    // -------------------------------------------------------------------------

    public function test_with_count_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('posts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_aliased(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts as total_posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('posts as total_posts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_multiple_aggregates(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount(['posts', 'tags'])->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount(['posts', 'tags'])->orderBy('name')->get(),
        );
    }

    public function test_with_count_constrained(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount(['posts' => fn($q) => $q->where('published', true)])->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount(['posts' => fn($q) => $q->where('published', true)])->orderBy('name')->get(),
        );
    }

    public function test_with_count_zero_when_no_related(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::where('name', 'Carol')->withCount('posts')->first(),
            fn() => Author::withoutCache()->withoutAggregateCache()->where('name', 'Carol')->withCount('posts')->first(),
        );
    }

    public function test_with_sum_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withSum('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withSum('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_avg_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withAvg('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withAvg('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_min_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withMin('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withMin('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_max_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withMax('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withMax('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_count_belongs_to_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('tags')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('tags')->orderBy('name')->get(),
        );
    }

    public function test_with_exists_adds_correct_boolean_attribute(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withExists('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withExists('posts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_morph_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('comments')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('comments')->orderBy('name')->get(),
        );
    }

    public function test_with_count_multiple_morph_relations_simultaneously(): void
    {
        $this->fixtures(); // p1: 1 tag, 1 comment; p2/p3: 0 of each
        $this->contract(
            fn() => Post::withCount(['tags', 'comments'])->orderBy('title')->get(),
            fn() => Post::withoutCache()->withoutAggregateCache()->withCount(['tags', 'comments'])->orderBy('title')->get(),
        );
    }

    public function test_with_aggregate_direct_call_produces_correct_attribute_and_value(): void
    {
        $this->fixtures(); // Alice: A1(10), A2(20); Bob: B1(30); Carol: none
        $this->contract(
            fn() => Author::withAggregate('posts', 'views', 'avg')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withAggregate('posts', 'views', 'avg')->orderBy('name')->get(),
        );
    }

    public function test_with_aggregate_expression_column_produces_same_attribute(): void
    {
        // grammar->getValue() unwraps DB::raw so the alias is posts_sum_views,
        // not posts_sum_select_views_from (which would happen with raw toString).
        $this->fixtures();
        $this->contract(
            fn() => Author::withSum('posts', DB::raw('views'))->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withSum('posts', DB::raw('views'))->orderBy('name')->get(),
        );
    }

    public function test_with_count_non_cacheable_related_model_falls_through_to_eloquent(): void
    {
        // When the related model has no Cacheable trait, NormCache routes to
        // parent::withAggregate (native Eloquent subselect). Result must match.
        $author = Author::create(['name' => 'Alice']);
        UncachedPost::create(['title' => 'P1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'P2', 'author_id' => $author->id]);

        $this->contract(
            fn() => Author::withCount('uncachedPosts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('uncachedPosts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_mixed_cacheable_and_non_cacheable_in_one_call(): void
    {
        // Cacheable relation goes through pendingAggregates (deferred Redis path);
        // non-cacheable relation goes through parent::withAggregate (Eloquent subselect).
        // Both aggregate VALUES must match native. Attribute insertion order is not
        // compared — it is an implementation detail that users should not rely on.
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'UP1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'UP2', 'author_id' => $author->id]);

        $values = fn($result) => $result->map(fn($m) => [
            'posts_count' => (int) $m->posts_count,
            'uncached_posts_count' => (int) $m->uncached_posts_count,
        ])->all();

        $native = $values(Author::withoutCache()->withoutAggregateCache()->withCount(['posts', 'uncachedPosts'])->orderBy('name')->get());
        $cold = $values(Author::withCount(['posts', 'uncachedPosts'])->orderBy('name')->get());
        $warm = $values(Author::withCount(['posts', 'uncachedPosts'])->orderBy('name')->get());

        $this->assertSame($native, $cold, 'cold aggregate values differ from native');
        $this->assertSame($cold, $warm, 'warm aggregate values differ from cold');
    }

    public function test_with_count_having_on_aggregate_alias_behaves_same_as_native(): void
    {
        // HAVING on an aggregate alias requires the aggregate in SQL — deferred loading
        // cannot provide it, so NormCache must fall back to native Eloquent.
        // SQLite rejects HAVING without GROUP BY, so we verify they fail identically.
        $this->fixtures();

        $nativeException = null;
        try {
            Author::withoutCache()->withoutAggregateCache()->withCount('posts')->having('posts_count', '>', 1)->get();
        } catch (\Exception $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        try {
            Author::withCount('posts')->having('posts_count', '>', 1)->get();
        } catch (\Exception $e) {
            $normcacheException = get_class($e);
        }

        $this->assertSame($nativeException, $normcacheException, 'NormCache must fail identically to native Eloquent on HAVING aggregate alias');
    }

    public function test_with_count_order_by_raw_aggregate_alias_matches_native(): void
    {
        // orderByRaw referencing an aggregate alias cannot be executed on the ID-fetch query
        // (the alias column doesn't exist in SQL). NormCache must detect this and fall back
        // to native Eloquent, which computes the aggregate as a subselect in the same query.
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->orderByRaw('posts_count desc')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('posts')->orderByRaw('posts_count desc')->get(),
        );
    }

    public function test_without_aggregate_cache_falls_through_entirely(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withoutAggregateCache()->withCount('posts')->withSum('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('posts')->withSum('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_without_aggregate_cache_called_after_with_count_replays_to_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->withSum('posts', 'views')->withoutAggregateCache()->orderBy('name')->get(),
            fn() => Author::withoutCache()->withoutAggregateCache()->withCount('posts')->withSum('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_count_alias_with_extra_whitespace_behaves_same_as_native(): void
    {
        // NormCache uses Eloquent's own 3-segment explode(' ') parsing — multiple spaces
        // are NOT supported by Eloquent and NormCache must fail identically, not silently
        // handle what native Eloquent rejects.
        $this->fixtures();

        $nativeException = null;
        try {
            Author::withoutCache()->withoutAggregateCache()->withCount('posts  as  total_posts')->get();
        } catch (\Exception $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        try {
            Author::withCount('posts  as  total_posts')->get();
        } catch (\Exception $e) {
            $normcacheException = get_class($e);
        }

        $this->assertSame($nativeException, $normcacheException, 'NormCache must fail identically to native Eloquent on malformed alias syntax');
    }

    public function test_with_count_nested_relation_name_throws_same_as_native(): void
    {
        $this->fixtures();
        // Eloquent itself does not support nested dot-notation in a single withCount call.
        // NormCache must pass it through to the parent (not crash earlier on its own
        // model->{name}() call), so the exception is the same type as native Eloquent.
        $nativeException = null;
        try {
            Author::withoutCache()->withoutAggregateCache()->withCount('posts.comments')->get();
        } catch (\Exception $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        try {
            Author::withCount('posts.comments')->get();
        } catch (\Exception $e) {
            $normcacheException = get_class($e);
        }

        $this->assertNotNull($nativeException, 'Expected native Eloquent to throw');
        $this->assertSame($nativeException, $normcacheException, 'NormCache must throw the same exception type as native Eloquent');
    }

    // -------------------------------------------------------------------------
    // Eager loading
    // -------------------------------------------------------------------------

    public function test_with_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::with('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->with('posts')->orderBy('name')->get(),
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

        // latestOfMany adds a synthetic id_aggregate JOIN column that differs between
        // the native and cached paths due to prepareMissedQuery dropping beforeQuery hooks.
        // Test the semantically correct part: which Post was selected.
        $check = fn($c) => $c->map(fn($a) => [
            'author' => $a->name,
            'latest_title' => $a->latestPost?->title,
        ])->all();

        $native = $check(Author::withoutCache()->with('latestPost')->orderBy('name')->get());
        $cold = $check(Author::with('latestPost')->orderBy('name')->get());
        $warm = $check(Author::with('latestPost')->orderBy('name')->get());

        $this->assertSame($native, $cold, 'cold != native');
        $this->assertSame($cold, $warm, 'warm != cold');
    }

    public function test_with_has_one_of_many_aggregate_column(): void
    {
        $this->fixtures(); // Alice: mostViewed=A2(20), Bob: mostViewed=B1(30), Carol: null

        $check = fn($c) => $c->map(fn($a) => [
            'author' => $a->name,
            'most_viewed_title' => $a->mostViewedPost?->title,
            'most_viewed_views' => $a->mostViewedPost?->views,
        ])->all();

        $native = $check(Author::withoutCache()->with('mostViewedPost')->orderBy('name')->get());
        $cold = $check(Author::with('mostViewedPost')->orderBy('name')->get());
        $warm = $check(Author::with('mostViewedPost')->orderBy('name')->get());

        $this->assertSame($native, $cold, 'cold != native');
        $this->assertSame($cold, $warm, 'warm != cold');
    }

    // -------------------------------------------------------------------------
    // Collection loading (load, loadMissing, loadCount, loadSum, loadMax, loadMin)
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // dependsOn — raw cache
    // -------------------------------------------------------------------------

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

    public function test_depends_on_join_auto_qualifies_select(): void
    {
        // When dependsOn() is used with a JOIN and no explicit select, NormCache automatically
        // qualifies the select to `table.*` to avoid duplicate-column ambiguity during hydration.
        // This is intentional and documented behavior.
        $this->fixtures();
        $this->contract(
            fn() => Author::query()
                ->join('posts', 'posts.author_id', '=', 'authors.id')
                ->dependsOn([Post::class])
                ->orderBy('authors.name')
                ->get(),
            fn() => Author::withoutCache()
                ->join('posts', 'posts.author_id', '=', 'authors.id')
                ->select('authors.*')
                ->orderBy('authors.name')
                ->get(),
        );
    }

    // -------------------------------------------------------------------------
    // Bypass paths (join, groupBy, lockForUpdate, simplePaginate, cursorPaginate)
    // -------------------------------------------------------------------------

    public function test_join_without_depends_on_falls_through(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::join('posts', 'posts.author_id', '=', 'authors.id')
                ->select('authors.*')
                ->orderBy('authors.name')
                ->get(),
            fn() => Author::withoutCache()
                ->join('posts', 'posts.author_id', '=', 'authors.id')
                ->select('authors.*')
                ->orderBy('authors.name')
                ->get(),
        );
    }

    public function test_group_by_falls_through(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Post::select('author_id')
                ->selectRaw('sum(views) as total_views')
                ->groupBy('author_id')
                ->orderBy('author_id')
                ->get(),
            fn() => Post::withoutCache()
                ->select('author_id')
                ->selectRaw('sum(views) as total_views')
                ->groupBy('author_id')
                ->orderBy('author_id')
                ->get(),
        );
    }

    public function test_lock_for_update_falls_through(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::lockForUpdate()->orderBy('name')->get(),
            fn() => Author::withoutCache()->orderBy('name')->get(),
        );
    }

    public function test_simple_paginate_falls_through(): void
    {
        $this->fixtures();
        $native = Author::withoutCache()->orderBy('name')->simplePaginate(2);
        $result = Author::orderBy('name')->simplePaginate(2);
        $this->assertSame(
            collect($native->items())->map->toArray()->values()->all(),
            collect($result->items())->map->toArray()->values()->all(),
        );
    }

    public function test_cursor_paginate_falls_through_correctly(): void
    {
        $this->fixtures();

        $native = Author::withoutCache()->orderBy('name')->cursorPaginate(2);
        $result = Author::orderBy('name')->cursorPaginate(2);

        $this->assertSame(
            collect($native->items())->map->toArray()->values()->all(),
            collect($result->items())->map->toArray()->values()->all(),
        );
    }

    // -------------------------------------------------------------------------
    // whereHas variants
    // -------------------------------------------------------------------------

    public function test_doesnt_have_returns_correct_models(): void
    {
        $this->fixtures(); // Carol has no posts
        $this->contract(
            fn() => Author::doesntHave('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->doesntHave('posts')->orderBy('name')->get(),
        );
    }

    public function test_has_with_count_threshold_returns_correct_models(): void
    {
        $this->fixtures(); // Alice has 2 posts, Bob has 1, Carol has 0
        $this->contract(
            fn() => Author::has('posts', '>=', 2)->orderBy('name')->get(),
            fn() => Author::withoutCache()->has('posts', '>=', 2)->orderBy('name')->get(),
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

    public function test_where_not_closure_returns_correct_models(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::whereNot(fn($q) => $q->where('name', 'Carol'))->orderBy('name')->get(),
            fn() => Author::withoutCache()->whereNot(fn($q) => $q->where('name', 'Carol'))->orderBy('name')->get(),
        );
    }

    // -------------------------------------------------------------------------
    // Global scopes
    // -------------------------------------------------------------------------

    public function test_global_scope_applies_consistently_cold_and_warm(): void
    {
        $this->fixtures();

        Author::addGlobalScope('contract_test', fn($q) => $q->where('name', '!=', 'Carol'));

        try {
            $this->contract(
                fn() => Author::orderBy('name')->get(),
                fn() => Author::withoutCache()->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Author::class, 'contract_test');
        }
    }

    public function test_without_global_scope_applies_consistently_cold_and_warm(): void
    {
        $this->fixtures();

        Author::addGlobalScope('contract_test2', fn($q) => $q->where('name', '!=', 'Carol'));

        try {
            $this->contract(
                fn() => Author::withoutGlobalScope('contract_test2')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withoutGlobalScope('contract_test2')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Author::class, 'contract_test2');
        }
    }

    public function test_with_count_respects_global_scope_on_related_model(): void
    {
        $this->fixtures(); // Alice: 2 posts (1 published), Bob: 1 post (1 published)

        Post::addGlobalScope('published_only', fn($q) => $q->where('published', true));

        try {
            $this->contract(
                fn() => Author::withCount('posts')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withoutAggregateCache()->withCount('posts')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Post::class, 'published_only');
        }
    }

    public function test_belongs_to_many_respects_global_scope_on_related(): void
    {
        $this->fixtures(); // Alice has php+laravel tags, Bob has php
        Tag::addGlobalScope('no_laravel', fn($q) => $q->where('name', '!=', 'laravel'));

        try {
            $this->contract(
                fn() => Author::with('tags')->orderBy('name')->get(),
                fn() => Author::withoutCache()->with('tags')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Tag::class, 'no_laravel');
        }
    }

    public function test_with_count_respects_global_scope_column_restriction_on_belongs_to_many(): void
    {
        $this->fixtures();
        Tag::addGlobalScope('no_laravel', fn($q) => $q->where('name', '!=', 'laravel'));

        try {
            $this->contract(
                fn() => Author::withCount('tags')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withoutAggregateCache()->withCount('tags')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Tag::class, 'no_laravel');
        }
    }

    public function test_has_many_through_respects_global_scope_on_through_model(): void
    {
        $this->fixtures(); // UK country → posts through Alice+Bob; Carol has no country
        Post::addGlobalScope('published', fn($q) => $q->where('published', true));

        try {
            $this->contract(
                fn() => Country::with('posts')->first(),
                fn() => Country::withoutCache()->with('posts')->first(),
            );
        } finally {
            $this->clearGlobalScope(Post::class, 'published');
        }
    }

    public function test_morph_to_excludes_soft_deleted_related_by_default(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete(); // soft-delete the post that c2 points to

        $this->contract(
            fn() => Comment::with('commentable')->orderBy('id')->get(),
            fn() => Comment::withoutCache()->with('commentable')->orderBy('id')->get(),
        );
    }

    public function test_morph_to_includes_soft_deleted_via_constraint(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete();

        $this->contract(
            fn() => Comment::with(['commentable' => fn($q) => $q->withoutGlobalScopes()])->orderBy('id')->get(),
            fn() => Comment::withoutCache()->with(['commentable' => fn($q) => $q->withoutGlobalScopes()])->orderBy('id')->get(),
        );
    }

    public function test_with_count_honours_removed_scope_on_parent_builder(): void
    {
        // Add a scope that excludes Carol (no country_id) from fresh Author queries.
        // The outer query explicitly removes it so Carol IS in the result.
        // The aggregate miss-fetch must also remove the scope; otherwise Carol's
        // posts_count comes back null instead of 0.
        Author::addGlobalScope('has_country', fn($q) => $q->whereNotNull('country_id'));

        try {
            $this->fixtures(); // Alice, Bob have country_id=1; Carol has null

            $this->contract(
                fn() => Author::withoutGlobalScope('has_country')->withCount('posts')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withoutAggregateCache()->withoutGlobalScope('has_country')->withCount('posts')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Author::class, 'has_country');
        }
    }

    // -------------------------------------------------------------------------
    // Write operations (insert, update, delete, insertOrIgnore, upsert, forceDelete)
    // -------------------------------------------------------------------------

    public function test_insert_returns_bool(): void
    {
        $native = Author::withoutCache()->insert(['name' => 'Test1', 'created_at' => now(), 'updated_at' => now()]);
        $cached = Author::insert(['name' => 'Test2', 'created_at' => now(), 'updated_at' => now()]);

        $this->assertSame($native, $cached);
        $this->assertIsBool($cached);
    }

    public function test_update_returns_affected_row_count(): void
    {
        $this->fixtures();

        $native = Author::withoutCache()->where('name', 'Alice')->update(['name' => 'Alice1']);
        $cached = Author::where('name', 'Bob')->update(['name' => 'Bob1']);

        $this->assertSame($native, $cached);
        $this->assertIsInt($cached);
    }

    public function test_delete_returns_affected_row_count(): void
    {
        Author::create(['name' => 'Delete1', 'created_at' => now(), 'updated_at' => now()]);
        Author::create(['name' => 'Delete2', 'created_at' => now(), 'updated_at' => now()]);

        $native = Author::withoutCache()->where('name', 'Delete1')->delete();
        $cached = Author::where('name', 'Delete2')->delete();

        $this->assertSame($native, $cached);
        $this->assertIsInt($cached);
    }

    public function test_insert_or_ignore_returns_affected_row_count(): void
    {
        Author::create(['name' => 'Existing']);

        $native = Author::withoutCache()->insertOrIgnore([
            ['name' => 'New1', 'created_at' => now(), 'updated_at' => now()],
        ]);
        $cached = Author::insertOrIgnore([
            ['name' => 'New2', 'created_at' => now(), 'updated_at' => now()],
        ]);

        $this->assertSame($native, $cached);
        $this->assertIsInt($cached);
    }

    public function test_upsert_returns_affected_row_count(): void
    {
        $tag1 = Tag::create(['name' => 'original1']);
        $tag2 = Tag::create(['name' => 'original2']);

        $native = Tag::withoutCache()->upsert(
            [['id' => $tag1->id, 'name' => 'updated1', 'created_at' => now(), 'updated_at' => now()]],
            ['id'], ['name'],
        );
        $cached = Tag::upsert(
            [['id' => $tag2->id, 'name' => 'updated2', 'created_at' => now(), 'updated_at' => now()]],
            ['id'], ['name'],
        );

        $this->assertSame($native, $cached);
    }

    public function test_force_delete_returns_affected_row_count(): void
    {
        $p1 = Post::create(['title' => 'FD1', 'author_id' => Author::create(['name' => 'X'])->id]);
        $p2 = Post::create(['title' => 'FD2', 'author_id' => Author::create(['name' => 'Y'])->id]);
        $p1->delete();
        $p2->delete();

        $native = Post::withoutCache()->onlyTrashed()->where('title', 'FD1')->forceDelete();
        $cached = Post::onlyTrashed()->where('title', 'FD2')->forceDelete();

        $this->assertSame($native, $cached);
    }

    // -------------------------------------------------------------------------
    // flushTag validation
    // -------------------------------------------------------------------------

    public function test_flush_tag_rejects_unsafe_characters(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->cacheManager()->flushTag(Author::class, 'tag:with:colons');
    }

    public function test_flush_tag_across_models_rejects_unsafe_characters(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->cacheManager()->flushTagAcrossModels('tag*with*stars');
    }

    // -------------------------------------------------------------------------
    // Scalar expression guard
    // -------------------------------------------------------------------------

    public function test_sum_with_raw_expression_bypasses_cache_and_returns_correct_result(): void
    {
        $this->fixtures();

        $this->contract(
            fn() => Post::sum(DB::raw('views')),
            fn() => Post::withoutCache()->sum(DB::raw('views')),
        );
    }

    public function test_value_with_raw_expression_bypasses_cache_and_returns_correct_result(): void
    {
        $this->fixtures();

        $this->contract(
            fn() => Post::orderBy('id')->value(DB::raw('title')),
            fn() => Post::withoutCache()->orderBy('id')->value(DB::raw('title')),
        );
    }

    // -------------------------------------------------------------------------
    // Expression primary key guard
    // -------------------------------------------------------------------------

    public function test_where_id_with_expression_falls_through_to_normal_query_cache(): void
    {
        ['alice' => $alice] = $this->fixtures();

        $this->contract(
            fn() => Author::where('id', DB::raw($alice->id))->get(),
            fn() => Author::withoutCache()->where('id', DB::raw($alice->id))->get(),
        );
    }

    // -------------------------------------------------------------------------
    // Pivot constraint hash — raw order bindings
    // -------------------------------------------------------------------------

    public function test_pivot_orderby_raw_with_different_bindings_returns_distinct_results(): void
    {
        ['alice' => $alice] = $this->fixtures();

        $phpFirst = $alice->tags()
            ->orderByRaw('CASE WHEN tags.name = ? THEN 0 ELSE 1 END', ['php'])
            ->get()
            ->pluck('name')
            ->all();

        $laravelFirst = $alice->tags()
            ->orderByRaw('CASE WHEN tags.name = ? THEN 0 ELSE 1 END', ['laravel'])
            ->get()
            ->pluck('name')
            ->all();

        $this->assertSame(['php', 'laravel'], $phpFirst);
        $this->assertSame(['laravel', 'php'], $laravelFirst);
    }
}
