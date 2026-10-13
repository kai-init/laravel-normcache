<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Integration\Contract\Concerns\CreatesEloquentContractFixtures;
use NormCache\Tests\TestCase;

/**
 * Contract tests for Eloquent read behavior.
 */
final class EloquentReadContractTest extends TestCase
{
    use CreatesEloquentContractFixtures;

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
        $this->missContract(
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

    public function test_depends_on_join_with_explicit_select_caches_and_matches_native(): void
    {
        // JOIN + dependsOn + explicit select caches as result and matches native behavior.
        $this->fixtures();
        $this->contract(
            fn() => Author::query()
                ->join('posts', 'posts.author_id', '=', 'authors.id')
                ->select('authors.*')
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

    public function test_join_infers_dependencies_without_depends_on(): void
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

    public function test_lock_for_update_falls_through(): void
    {
        $this->fixtures();
        $this->bypassContract(
            fn() => Author::lockForUpdate()->orderBy('name')->get(),
            fn() => Author::withoutCache()->orderBy('name')->get(),
            reason: 'write_pdo',
        );
    }

    public function test_chunk_returns_native_results(): void
    {
        $this->fixtures();

        $this->contract(
            fn(): array => $this->chunkedNames(Author::orderBy('id')),
            fn(): array => $this->chunkedNames(Author::withoutCache()->orderBy('id')),
        );
    }

    public function test_lazy_returns_native_results(): void
    {
        $this->fixtures();

        $this->contract(
            fn(): array => Author::orderBy('id')->lazy(2)->pluck('name')->all(),
            fn(): array => Author::withoutCache()->orderBy('id')->lazy(2)->pluck('name')->all(),
        );
    }

    public function test_chunk_by_id_returns_native_results(): void
    {
        $this->fixtures();

        $this->contract(
            fn(): array => $this->chunkedByIdNames(Author::query()),
            fn(): array => $this->chunkedByIdNames(Author::withoutCache()),
        );
    }

    public function test_each_returns_native_results(): void
    {
        $this->fixtures();

        $this->contract(
            fn(): array => $this->eachNames(Author::orderBy('id')),
            fn(): array => $this->eachNames(Author::withoutCache()->orderBy('id')),
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

    public function test_query_id_cache_preserves_large_integer_primary_keys(): void
    {
        $largeId = 9007199254740993; // 2^53 + 1 — first integer lossy in float64 / cjson
        DB::table('authors')->insert([
            'id' => $largeId,
            'name' => 'BigId',
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->contract(
            fn() => Author::where('name', 'BigId')->get(),
            fn() => Author::withoutCache()->where('name', 'BigId')->get(),
        );
    }

    public function test_value_with_raw_expression_is_cached_and_returns_correct_result(): void
    {
        $this->fixtures();

        $this->contract(
            fn() => Post::orderBy('id')->value(DB::raw('title')),
            fn() => Post::withoutCache()->orderBy('id')->value(DB::raw('title')),
        );
    }

    public function test_where_id_with_expression_falls_through_to_normal_query_cache(): void
    {
        ['alice' => $alice] = $this->fixtures();

        $this->contract(
            fn() => Author::where('id', DB::raw($alice->id))->get(),
            fn() => Author::withoutCache()->where('id', DB::raw($alice->id))->get(),
        );
    }

    private function chunkedNames($query): array
    {
        $names = [];

        $query->chunk(2, function ($authors) use (&$names): void {
            foreach ($authors as $author) {
                $names[] = $author->name;
            }
        });

        return $names;
    }

    private function chunkedByIdNames($query): array
    {
        $names = [];

        $query->chunkById(2, function ($authors) use (&$names): void {
            foreach ($authors as $author) {
                $names[] = $author->name;
            }
        });

        return $names;
    }

    private function eachNames($query): array
    {
        $names = [];

        $query->each(function ($author) use (&$names): void {
            $names[] = $author->name;
        }, 2);

        return $names;
    }
}
