<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\Integration\Contract\Concerns\CreatesEloquentContractFixtures;
use NormCache\Tests\TestCase;

/**
 * Contract tests for Eloquent aggregate behavior.
 */
final class EloquentAggregateContractTest extends TestCase
{
    use CreatesEloquentContractFixtures;

    public function test_aggregate_blob_key_includes_selected_columns(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $idQuery = fn() => Author::select('id')->withCount('posts')->where('id', $alice->id)->get();
        $idNative = fn() => Author::withoutCache()->select('id')->withCount('posts')->where('id', $alice->id)->get();
        $nameQuery = fn() => Author::select('name')->withCount('posts')->where('id', $alice->id)->get();
        $nameNative = fn() => Author::withoutCache()->select('name')->withCount('posts')->where('id', $alice->id)->get();

        $this->contract($idQuery, $idNative);
        $idOnly = $idQuery()->first();
        $this->contract($nameQuery, $nameNative);
        $nameOnly = $nameQuery()->first();

        $this->assertArrayHasKey('id', $idOnly->getAttributes(), 'id-only projection must contain id');
        $this->assertArrayNotHasKey('name', $idOnly->getAttributes(), 'id-only projection must not contain name from other query blob');
        $this->assertArrayHasKey('name', $nameOnly->getAttributes(), 'name-only projection must contain name');
        $this->assertArrayNotHasKey('id', $nameOnly->getAttributes(), 'name-only projection must not contain id from other query blob');
    }

    public function test_aggregate_blob_key_includes_relation_name(): void
    {
        ['alice' => $alice] = $this->fixtures();

        $this->contract(
            fn() => Author::withCount('posts')->where('id', $alice->id)->get(),
            fn() => Author::withoutCache()->withCount('posts')->where('id', $alice->id)->get(),
        );
        $this->contract(
            fn() => Author::withCount('firstPost')->where('id', $alice->id)->get(),
            fn() => Author::withoutCache()->withCount('firstPost')->where('id', $alice->id)->get(),
        );
    }

    public function test_aggregate_constraint_with_where_has_falls_back_to_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount(['posts' => fn($q) => $q->whereHas('tags')])->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount(['posts' => fn($q) => $q->whereHas('tags')])->orderBy('name')->get(),
        );
    }

    public function test_aggregate_blob_respects_explicit_depends_on(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);
        $query = fn() => Author::dependsOn([Tag::class])
            ->withCount('posts')
            ->where('id', $alice->id)
            ->get();
        $native = fn() => Author::withoutCache()
            ->withCount('posts')
            ->where('id', $alice->id)
            ->get();

        $this->contract(
            $query,
            $native,
            mutate: fn() => Tag::create(['name' => 'dummy']),
        );
    }

    public function test_with_count_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount('posts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_aliased(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts as total_posts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount('posts as total_posts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_multiple_aggregates(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount(['posts', 'tags'])->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount(['posts', 'tags'])->orderBy('name')->get(),
        );
    }

    public function test_with_count_constrained(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount(['posts' => fn($q) => $q->where('published', true)])->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount(['posts' => fn($q) => $q->where('published', true)])->orderBy('name')->get(),
        );
    }

    public function test_with_count_zero_when_no_related(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::where('name', 'Carol')->withCount('posts')->first(),
            fn() => Author::withoutCache()->where('name', 'Carol')->withCount('posts')->first(),
        );
    }

    public function test_with_sum_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withSum('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withSum('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_sum_custom_alias_matches_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withSum('posts as total_views', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withSum('posts as total_views', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_avg_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withAvg('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withAvg('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_min_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withMin('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withMin('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_with_max_has_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withMax('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withMax('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_multiple_aggregate_functions_on_same_relation_match_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withSum('posts', 'views')->withAvg('posts', 'views')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withSum('posts', 'views')->withAvg('posts', 'views')->orderBy('name')->get(),
        );
    }

    public function test_aggregate_alias_collision_with_real_attribute_matches_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts as name')->orderBy('id')->get(),
            fn() => Author::withoutCache()->withCount('posts as name')->orderBy('id')->get(),
        );
    }

    public function test_with_count_belongs_to_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('tags')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount('tags')->orderBy('name')->get(),
        );
    }

    public function test_with_count_belongs_to_many_invalidates_when_pivot_changes(): void
    {
        ['carol' => $carol, 'php' => $php] = $this->fixtures();

        $this->contract(
            fn() => Author::withCount('tags')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount('tags')->orderBy('name')->get(),
            mutate: fn() => $carol->tags()->attach($php->id),
        );
    }

    public function test_with_exists_requires_declared_dependencies(): void
    {
        $this->fixtures();
        $query = fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->withExists('posts')
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

    public function test_with_count_morph_many(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('comments')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount('comments')->orderBy('name')->get(),
        );
    }

    public function test_with_count_multiple_morph_relations_simultaneously(): void
    {
        $this->fixtures(); // p1: 1 tag, 1 comment; p2/p3: 0 of each
        $this->contract(
            fn() => Post::withCount(['tags', 'comments'])->orderBy('title')->get(),
            fn() => Post::withoutCache()->withCount(['tags', 'comments'])->orderBy('title')->get(),
        );
    }

    public function test_with_aggregate_direct_call_produces_correct_attribute_and_value(): void
    {
        $this->fixtures(); // Alice: A1(10), A2(20); Bob: B1(30); Carol: none
        $this->contract(
            fn() => Author::withAggregate('posts', 'views', 'avg')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withAggregate('posts', 'views', 'avg')->orderBy('name')->get(),
        );
    }

    public function test_with_aggregate_expression_column_produces_same_attribute(): void
    {
        // grammar->getValue() unwraps DB::raw so the alias matches native behavior (e.g. posts_sum_views).
        $this->fixtures();
        $this->contract(
            fn() => Author::withSum('posts', DB::raw('views'))->orderBy('name')->get(),
            fn() => Author::withoutCache()->withSum('posts', DB::raw('views'))->orderBy('name')->get(),
        );
    }

    public function test_with_count_non_cacheable_related_model_infers_table_dependency(): void
    {
        // The related model does not need Cacheable for its table dependency to be inferred.
        $author = Author::create(['name' => 'Alice']);
        UncachedPost::create(['title' => 'P1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'P2', 'author_id' => $author->id]);

        $this->contract(
            fn() => Author::withCount('uncachedPosts')->orderBy('name')->get(),
            fn() => Author::withoutCache()->withCount('uncachedPosts')->orderBy('name')->get(),
        );
    }

    public function test_with_count_mixed_cacheable_and_non_cacheable_in_one_call(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'UP1', 'author_id' => $author->id]);
        UncachedPost::create(['title' => 'UP2', 'author_id' => $author->id]);

        $values = fn($result) => $result->map(fn($m) => [
            'posts_count' => (int) $m->posts_count,
            'uncached_posts_count' => (int) $m->uncached_posts_count,
        ])->all();

        $this->contract(
            fn() => $values(Author::withCount(['posts', 'uncachedPosts'])->orderBy('name')->get()),
            fn() => $values(Author::withoutCache()->withCount(['posts', 'uncachedPosts'])->orderBy('name')->get()),
        );
    }

    public function test_with_count_having_raw_on_aggregate_alias_behaves_same_as_native(): void
    {
        // havingRaw stores the SQL in a 'sql' key, not 'column' — the guard must check both.
        $this->fixtures();

        $nativeException = null;
        $nativeResult = null;
        try {
            $nativeResult = Author::withoutCache()->withCount('posts')->havingRaw('posts_count > 0')->get();
        } catch (\Throwable $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        $normcacheResult = null;
        try {
            $normcacheResult = Author::withCount('posts')->havingRaw('posts_count > 0')->get();
        } catch (\Throwable $e) {
            $normcacheException = get_class($e);
        }

        $this->assertSame($nativeException, $normcacheException, 'NormCache must fail identically to native Eloquent on havingRaw aggregate alias');

        if ($nativeResult !== null && $normcacheResult !== null) {
            $this->assertSame(
                $this->normalize($nativeResult),
                $this->normalize($normcacheResult),
                'NormCache result must match native Eloquent on havingRaw aggregate alias',
            );
        }
    }

    public function test_with_count_having_on_aggregate_alias_behaves_same_as_native(): void
    {
        $this->fixtures();

        $nativeException = null;
        try {
            Author::withoutCache()->withCount('posts')->having('posts_count', '>', 1)->get();
        } catch (\Throwable $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        try {
            Author::withCount('posts')->having('posts_count', '>', 1)->get();
        } catch (\Throwable $e) {
            $normcacheException = get_class($e);
        }

        $this->assertSame($nativeException, $normcacheException, 'NormCache must fail identically to native Eloquent on HAVING aggregate alias');
    }

    public function test_with_count_order_by_raw_aggregate_alias_is_cached(): void
    {
        // A raw order using only the generated aggregate alias adds no hidden table dependency.
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->orderByRaw('posts_count desc')->get(),
            fn() => Author::withoutCache()->withCount('posts')->orderByRaw('posts_count desc')->get(),
        );
    }

    public function test_with_count_alias_with_extra_whitespace_behaves_same_as_native(): void
    {
        // Eloquent fails on multiple spaces in aliases; NormCache must fail identically.
        $this->fixtures();

        $nativeException = null;
        try {
            Author::withoutCache()->withCount('posts  as  total_posts')->get();
        } catch (\Throwable $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        try {
            Author::withCount('posts  as  total_posts')->get();
        } catch (\Throwable $e) {
            $normcacheException = get_class($e);
        }

        $this->assertSame($nativeException, $normcacheException, 'NormCache must fail identically to native Eloquent on malformed alias syntax');
    }

    public function test_with_count_nested_relation_name_throws_same_as_native(): void
    {
        $this->fixtures();
        // Native Eloquent does not support nested dot-notation in a single withCount; NormCache must throw the same exception.
        $nativeException = null;
        try {
            Author::withoutCache()->withCount('posts.comments')->get();
        } catch (\Throwable $e) {
            $nativeException = get_class($e);
        }

        $normcacheException = null;
        try {
            Author::withCount('posts.comments')->get();
        } catch (\Throwable $e) {
            $normcacheException = get_class($e);
        }

        $this->assertNotNull($nativeException, 'Expected native Eloquent to throw');
        $this->assertSame($nativeException, $normcacheException, 'NormCache must throw the same exception type as native Eloquent');
    }

    public function test_result_depends_on_with_group_by_and_withcount_skips_aggregate_on_null_pk_models(): void
    {
        if (DB::connection()->getDriverName() !== 'sqlite') {
            $this->markTestSkipped('This null-primary-key grouped aggregate shape is only valid on SQLite.');
        }

        // GROUP BY result dependsOn results lack primary keys — RelationAggregateLoader must skip aggregate loading.
        $alice = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'P1', 'author_id' => $alice->id]);
        Comment::create(['body' => 'Hi', 'commentable_type' => Post::class, 'commentable_id' => $post->id]);

        $run = fn() => Post::select('author_id', DB::raw('COUNT(*) as post_count'))
            ->groupBy('author_id')
            ->dependsOn([Post::class])
            ->withCount('comments')
            ->get();
        $native = fn() => Post::withoutCache()
            ->select('author_id', DB::raw('COUNT(*) as post_count'))
            ->groupBy('author_id')
            ->withCount('comments')
            ->get();

        $this->contract($run, $native);
        $this->assertCount(1, $run());
    }

    public function test_group_by_result_is_cached(): void
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

    public function test_with_count_pluck_aggregate_alias_matches_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->orderBy('name')->pluck('posts_count', 'name')->all(),
            fn() => Author::withoutCache()->withCount('posts')->orderBy('name')->pluck('posts_count', 'name')->all(),
        );
    }

    public function test_with_count_value_aggregate_alias_matches_native(): void
    {
        $this->fixtures();
        $this->contract(
            fn() => Author::withCount('posts')->orderBy('name')->value('posts_count'),
            fn() => Author::withoutCache()->withCount('posts')->orderBy('name')->value('posts_count'),
        );
    }

    public function test_with_count_cursor_aggregate_alias_matches_native(): void
    {
        $this->fixtures();
        $this->databaseContract(
            fn() => Author::withCount('posts')->orderBy('name')->cursor()->map->posts_count->all(),
            fn() => Author::withoutCache()->withCount('posts')->orderBy('name')->cursor()->map->posts_count->all(),
        );
    }

    public function test_sum_with_raw_expression_is_cached_and_returns_correct_result(): void
    {
        $this->fixtures();

        $this->contract(
            fn() => Post::sum(DB::raw('views')),
            fn() => Post::withoutCache()->sum(DB::raw('views')),
        );
    }
}
