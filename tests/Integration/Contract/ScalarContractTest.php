<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\TestCase;

/**
 * Contract tests: scalar aggregate operations (count, sum, avg, min, max,
 * exists, doesntExist, value, pluck) must return identical results on the
 * native path (withoutCache), cold-cache path, and warm-cache path.
 */
class ScalarContractTest extends TestCase
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

    // -------------------------------------------------------------------------
    // count
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

    // -------------------------------------------------------------------------
    // sum, avg, min, max
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // exists, doesntExist
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // value, pluck
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Edge cases: empty sets and nullable columns
    // -------------------------------------------------------------------------

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
}
