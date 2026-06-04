<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Query\Builder;
use NormCache\CacheableBuilder;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class QueryHasherTest extends TestCase
{
    private function makeBuilder(): Builder
    {
        return $this->app['db']->query();
    }

    private function makeEloquentBuilder(): CacheableBuilder
    {
        return Author::query();
    }

    public function test_same_query_produces_identical_hash(): void
    {
        $a = $this->makeBuilder()->from('posts')->where('id', 1);
        $b = $this->makeBuilder()->from('posts')->where('id', 1);

        $this->assertSame(QueryHasher::fromQuery($a), QueryHasher::fromQuery($b));
    }

    public function test_different_bindings_produce_different_hash(): void
    {
        $a = $this->makeBuilder()->from('posts')->where('id', 1);
        $b = $this->makeBuilder()->from('posts')->where('id', 2);

        $this->assertNotSame(QueryHasher::fromQuery($a), QueryHasher::fromQuery($b));
    }

    public function test_different_sql_produces_different_hash(): void
    {
        $a = $this->makeBuilder()->from('posts')->where('id', 1);
        $b = $this->makeBuilder()->from('authors')->where('id', 1);

        $this->assertNotSame(QueryHasher::fromQuery($a), QueryHasher::fromQuery($b));
    }

    public function test_use_write_pdo_produces_different_hash(): void
    {
        $read = $this->makeBuilder()->from('authors')->where('id', 1);
        $write = $this->makeBuilder()->from('authors')->where('id', 1)->useWritePdo();

        $this->assertNotSame(QueryHasher::fromQuery($read), QueryHasher::fromQuery($write));
    }

    public function test_it_hashes_raw_string(): void
    {
        $hash = QueryHasher::hash('some data');
        $this->assertIsString($hash);
        $this->assertEquals(16, strlen($hash));
        $this->assertSame(hash('xxh3', 'some data'), $hash);
    }

    public function test_pagination_count_hash_differs_from_normalized_query_hash(): void
    {
        $builder = $this->makeEloquentBuilder()->where('id', 1);

        $this->assertNotSame(
            QueryHasher::forNormalizedQuery($builder),
            QueryHasher::forPaginationCountQuery($builder)
        );
    }

    public function test_pagination_count_hash_is_stable_for_identical_queries(): void
    {
        $a = $this->makeEloquentBuilder()->where('id', 1);
        $b = $this->makeEloquentBuilder()->where('id', 1);

        $this->assertSame(
            QueryHasher::forPaginationCountQuery($a),
            QueryHasher::forPaginationCountQuery($b)
        );
    }

    public function test_pagination_count_hash_strips_column_selection(): void
    {
        $a = $this->makeEloquentBuilder()->where('id', 1)->select('id');
        $b = $this->makeEloquentBuilder()->where('id', 1)->select('name');

        $this->assertSame(
            QueryHasher::forPaginationCountQuery($a),
            QueryHasher::forPaginationCountQuery($b)
        );
    }

    public function test_pagination_count_hash_differs_when_where_clause_differs(): void
    {
        $a = $this->makeEloquentBuilder()->where('id', 1);
        $b = $this->makeEloquentBuilder()->where('id', 2);

        $this->assertNotSame(
            QueryHasher::forPaginationCountQuery($a),
            QueryHasher::forPaginationCountQuery($b)
        );
    }
}
