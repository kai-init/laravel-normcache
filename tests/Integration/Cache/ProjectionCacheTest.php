<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class ProjectionCacheTest extends TestCase
{
    #[DataProvider('projections')]
    public function test_projections_cache_their_own_sql_result(string $shape): void
    {
        $author = Author::create(['name' => 'Zulu']);
        Author::create(['name' => 'Alpha']);
        Author::orderBy('id')->get();
        Author::find($author->id);

        $build = fn(): Builder => match ($shape) {
            'primary key' => Author::whereKey($author->id)->select('name'),
            'range' => Author::orderBy('id')->select('id', 'name'),
            'qualified' => Author::orderBy('authors.id')->select('authors.name'),
            'alias' => Author::from('authors as a')->orderBy('a.id')->select('a.name'),
            'expression' => Author::orderBy('id')->selectRaw('upper(name) as heading'),
            'empty' => Author::where('id', -1)->select('name'),
            'tag' => Author::orderBy('id')->select('name')->tag('names'),
            'ttl' => Author::orderBy('id')->select('name')->ttl(60),
            'positional' => Author::select('name', 'id')->orderByRaw('1')->limit(1),
        };
        $expected = $build()->withoutCache()->get()->toArray();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $build()->get()->toArray();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame($expected, $actual);
        $this->assertCount(1, $queries, 'A different SELECT list has its own cold cache entry.');
        $this->assertSame($build()->toSql(), $queries[0]['query']);
        $this->assertWarmCacheHit(fn() => $build()->get());

        $author->update(['name' => 'Changed']);
        $this->assertColdCacheMiss(fn() => $build()->get());
        $this->assertSame($build()->withoutCache()->get()->toArray(), $build()->get()->toArray());
        $this->assertWarmCacheHit(fn() => $build()->get());
    }

    public static function projections(): array
    {
        return array_map(fn(string $shape): array => [$shape], [
            'primary key', 'range', 'qualified', 'alias', 'expression', 'empty', 'tag', 'ttl', 'positional',
        ]);
    }

    public function test_projected_queries_keep_soft_delete_visibility_in_sql(): void
    {
        $author = Author::create(['name' => 'Author']);
        $post = Post::create(['title' => 'Deleted', 'author_id' => $author->id]);
        $post->delete();
        Post::withTrashed()->find($post->id);

        foreach ([Post::query(), Post::onlyTrashed(), Post::withTrashed()] as $query) {
            $read = fn() => (clone $query)->select('title')->find($post->id);
            $this->contract($read, $read);
        }
    }

    public function test_invalid_projected_columns_preserve_database_errors(): void
    {
        Author::create(['name' => 'Author']);
        Author::get();

        $this->expectException(QueryException::class);
        Author::select('authors.missing_column')->get();
    }
}
