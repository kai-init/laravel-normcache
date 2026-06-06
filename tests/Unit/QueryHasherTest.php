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

    public function test_order_insensitive_scalar_hash_strips_order_clauses(): void
    {
        $plain = $this->makeEloquentBuilder()->where('id', '>', 0);

        $this->assertSame(
            QueryHasher::forScalarQuery($plain, $plain->toBase(), 'count', ['*']),
            QueryHasher::forScalarQuery($plain, $plain->toBase(), 'count', ['*'])
        );

        $a = $this->makeEloquentBuilder()->orderBy('name');
        $b = $this->makeEloquentBuilder()->orderBy('id');

        $this->assertSame(
            QueryHasher::forScalarQuery($a, $a->toBase(), 'count', ['*']),
            QueryHasher::forScalarQuery($b, $b->toBase(), 'count', ['*'])
        );

        $this->assertSame(
            QueryHasher::forScalarQuery($a, $a->toBase(), 'sum', ['id']),
            QueryHasher::forScalarQuery($b, $b->toBase(), 'sum', ['id'])
        );

        $this->assertSame(
            QueryHasher::forScalarQuery($a, $a->toBase(), 'exists', []),
            QueryHasher::forScalarQuery($b, $b->toBase(), 'exists', [])
        );
    }

    public function test_order_sensitive_scalar_hash_keeps_order_clauses(): void
    {
        $a = $this->makeEloquentBuilder()->orderBy('name');
        $b = $this->makeEloquentBuilder()->orderBy('id');

        $this->assertNotSame(
            QueryHasher::forScalarQuery($a, $a->toBase(), 'value', ['name']),
            QueryHasher::forScalarQuery($b, $b->toBase(), 'value', ['name'])
        );

        $this->assertNotSame(
            QueryHasher::forScalarQuery($a, $a->toBase(), 'pluck', ['name']),
            QueryHasher::forScalarQuery($b, $b->toBase(), 'pluck', ['name'])
        );
    }

    public function test_for_relation_query_strips_specific_key(): void
    {
        $a = $this->makeEloquentBuilder()->where('author_id', 1)->where('active', true);
        $b = $this->makeEloquentBuilder()->where('author_id', 2)->where('active', true);

        // Should be same because author_id is stripped
        $this->assertSame(
            QueryHasher::forRelationQuery($a, 'author_id'),
            QueryHasher::forRelationQuery($b, 'author_id')
        );

        $c = $this->makeEloquentBuilder()->where('author_id', 1)->where('active', false);

        // Should be different because active is NOT stripped
        $this->assertNotSame(
            QueryHasher::forRelationQuery($a, 'author_id'),
            QueryHasher::forRelationQuery($c, 'author_id')
        );
    }

    public function test_normalize_value_for_hash_is_recursive(): void
    {
        $subquery = $this->makeBuilder()->from('users')->select('id')->where('active', true);
        $value = [
            'nested' => [
                'query' => $subquery,
                'date' => new \DateTime('2023-01-01 12:00:00'),
            ],
        ];

        $normalized = QueryHasher::normalizeValueForHash($value);

        $this->assertIsArray($normalized);
        $this->assertArrayHasKey('nested', $normalized);
        $this->assertArrayHasKey('query', $normalized['nested']);
        $this->assertEquals('select "id" from "users" where "active" = ?', $normalized['nested']['query']['sql']);
        $this->assertEquals('2023-01-01 12:00:00', $normalized['nested']['date']);
    }
}
