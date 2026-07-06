<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Query\Builder;
use NormCache\CacheableBuilder;
use NormCache\Support\QueryHasher;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\UnitTestCase;

class QueryHasherTest extends UnitTestCase
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
            QueryHasher::forNormalizedQuery($builder, $builder->toBase()),
            QueryHasher::forPaginationCountQuery($builder, $builder->toBase())
        );
    }

    public function test_pagination_count_hash_is_stable_for_identical_queries(): void
    {
        $a = $this->makeEloquentBuilder()->where('id', 1);
        $b = $this->makeEloquentBuilder()->where('id', 1);

        $this->assertSame(
            QueryHasher::forPaginationCountQuery($a, $a->toBase()),
            QueryHasher::forPaginationCountQuery($b, $b->toBase())
        );
    }

    public function test_pagination_count_hash_strips_column_selection(): void
    {
        $a = $this->makeEloquentBuilder()->where('id', 1)->select('id');
        $b = $this->makeEloquentBuilder()->where('id', 1)->select('name');

        $this->assertSame(
            QueryHasher::forPaginationCountQuery($a, $a->toBase()),
            QueryHasher::forPaginationCountQuery($b, $b->toBase())
        );
    }

    public function test_pagination_count_hash_differs_when_where_clause_differs(): void
    {
        $a = $this->makeEloquentBuilder()->where('id', 1);
        $b = $this->makeEloquentBuilder()->where('id', 2);

        $this->assertNotSame(
            QueryHasher::forPaginationCountQuery($a, $a->toBase()),
            QueryHasher::forPaginationCountQuery($b, $b->toBase())
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
            QueryHasher::forRelationQuery('author_id', $a->toBase()),
            QueryHasher::forRelationQuery('author_id', $b->toBase())
        );

        $c = $this->makeEloquentBuilder()->where('author_id', 1)->where('active', false);

        // Should be different because active is NOT stripped
        $this->assertNotSame(
            QueryHasher::forRelationQuery('author_id', $a->toBase()),
            QueryHasher::forRelationQuery('author_id', $c->toBase())
        );
    }

    public function test_relation_hash_same_for_different_fk_batch_sizes(): void
    {
        $a = $this->makeEloquentBuilder()->whereIn('author_id', [1, 2, 3])->where('active', true);
        $b = $this->makeEloquentBuilder()->whereIn('author_id', [4, 5])->where('active', true);

        $this->assertSame(
            QueryHasher::forRelationQuery('author_id', $a->toBase()),
            QueryHasher::forRelationQuery('author_id', $b->toBase())
        );
    }

    public function test_relation_hash_captures_basic_where_value(): void
    {
        $a = $this->makeEloquentBuilder()->where('author_id', 1)->where('status', 1);
        $b = $this->makeEloquentBuilder()->where('author_id', 1)->where('status', 2);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('author_id', $a->toBase()),
            QueryHasher::forRelationQuery('author_id', $b->toBase())
        );
    }

    public function test_relation_hash_captures_where_in_values(): void
    {
        $a = $this->makeEloquentBuilder()->whereIn('type', [1, 2]);
        $b = $this->makeEloquentBuilder()->whereIn('type', [3, 4]);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_captures_where_between_values(): void
    {
        $a = $this->makeEloquentBuilder()->whereBetween('age', [18, 30]);
        $b = $this->makeEloquentBuilder()->whereBetween('age', [25, 40]);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_captures_raw_order_bindings(): void
    {
        $a = $this->makeEloquentBuilder()->orderByRaw('CASE WHEN name = ? THEN 0 ELSE 1 END', ['Alice']);
        $b = $this->makeEloquentBuilder()->orderByRaw('CASE WHEN name = ? THEN 0 ELSE 1 END', ['Bob']);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_distinguishes_null_check_types(): void
    {
        $a = $this->makeEloquentBuilder()->whereNull('deleted_at');
        $b = $this->makeEloquentBuilder()->whereNotNull('deleted_at');

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_distinguishes_null_check_columns(): void
    {
        $a = $this->makeEloquentBuilder()->whereNull('deleted_at');
        $b = $this->makeEloquentBuilder()->whereNull('published_at');

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_captures_nested_where_difference(): void
    {
        $a = $this->makeEloquentBuilder()->where(fn($q) => $q->where('active', true)->where('type', 1));
        $b = $this->makeEloquentBuilder()->where(fn($q) => $q->where('active', true)->where('type', 2));

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_captures_exists_subquery_difference(): void
    {
        $sub1 = $this->makeBuilder()->from('posts')->where('published', true);
        $sub2 = $this->makeBuilder()->from('posts')->where('published', false);

        $a = $this->makeEloquentBuilder()->whereExists($sub1);
        $b = $this->makeEloquentBuilder()->whereExists($sub2);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_is_stable(): void
    {
        $builder = $this->makeEloquentBuilder()->where('author_id', 1)->where('active', true);

        $this->assertSame(
            QueryHasher::forRelationQuery('author_id', $builder->toBase()),
            QueryHasher::forRelationQuery('author_id', $builder->toBase())
        );
    }

    public function test_relation_hash_stable_when_only_fk_where_present(): void
    {
        $a = $this->makeEloquentBuilder()->where('author_id', 99);
        $b = $this->makeEloquentBuilder()->where('author_id', 42);

        $this->assertSame(
            QueryHasher::forRelationQuery('author_id', $a->toBase()),
            QueryHasher::forRelationQuery('author_id', $b->toBase())
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

    public function test_relation_hash_differs_for_where_column_constraints(): void
    {
        $a = $this->makeEloquentBuilder()->whereColumn('a', '=', 'b');
        $b = $this->makeEloquentBuilder()->whereColumn('c', '=', 'd');

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_differs_for_where_integer_in_raw_values(): void
    {
        $a = $this->makeEloquentBuilder()->whereIntegerInRaw('status', [1, 2, 3]);
        $b = $this->makeEloquentBuilder()->whereIntegerInRaw('status', [4, 5, 6]);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_differs_for_where_integer_not_in_raw_values(): void
    {
        $a = $this->makeEloquentBuilder()->whereIntegerNotInRaw('status', [1, 2]);
        $b = $this->makeEloquentBuilder()->whereIntegerNotInRaw('status', [3, 4]);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_differs_for_where_between_columns(): void
    {
        $a = $this->makeEloquentBuilder()->whereBetweenColumns('price', ['min_price', 'max_price']);
        $b = $this->makeEloquentBuilder()->whereBetweenColumns('price', ['low', 'high']);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_differs_for_where_json_contains_value(): void
    {
        $a = $this->makeEloquentBuilder()->whereJsonContains('settings->theme', 'dark');
        $b = $this->makeEloquentBuilder()->whereJsonContains('settings->theme', 'light');

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_relation_hash_differs_for_where_row_values_constraints(): void
    {
        $a = $this->makeEloquentBuilder()->whereRowValues(['first_name', 'last_name'], '=', ['John', 'Doe']);
        $b = $this->makeEloquentBuilder()->whereRowValues(['first_name', 'last_name'], '=', ['Jane', 'Smith']);

        $this->assertNotSame(
            QueryHasher::forRelationQuery('fake_fk', $a->toBase()),
            QueryHasher::forRelationQuery('fake_fk', $b->toBase())
        );
    }

    public function test_query_hash_normalizes_binary_string_bindings(): void
    {
        $a = $this->makeEloquentBuilder()->where('data', "\x80\x81\x82\xFF");
        $b = $this->makeEloquentBuilder()->where('data', "\x83\x84\x85");

        // Must not throw JsonException from json_encode on invalid UTF-8
        $hashA = QueryHasher::forRelationQuery('fake_fk', $a->toBase());
        $hashB = QueryHasher::forRelationQuery('fake_fk', $b->toBase());

        $this->assertNotSame($hashA, $hashB);
    }
}
