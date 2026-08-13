<?php

namespace NormCache\Tests\Unit\Planning;

use Illuminate\Support\Facades\DB;
use NormCache\Database\QueryBuilder;
use NormCache\Planning\PredicateColumnExtractor;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\UnitTestCase;

final class PredicateColumnExtractorTest extends UnitTestCase
{
    private PredicateColumnExtractor $extractor;

    protected function setUp(): void
    {
        parent::setUp();

        $this->extractor = new PredicateColumnExtractor;
    }

    private function builder(): QueryBuilder
    {
        return RawPost::query()->toBase()->from('posts');
    }

    public function test_extracts_where_and_order_columns(): void
    {
        $query = $this->builder()->where('length', '>', 10)->orderBy('title');

        $this->assertSame(['length', 'title'], $this->extractor->extract($query));
    }

    public function test_descends_into_nested_wheres(): void
    {
        $query = $this->builder()->where(function ($q) {
            $q->where('album_id', 1)->orWhere('artist_id', 2);
        });

        $this->assertSame(['album_id', 'artist_id'], $this->extractor->extract($query));
    }

    public function test_strips_table_qualifiers(): void
    {
        $query = $this->builder()->where('posts.album_id', 1);

        $this->assertSame(['album_id'], $this->extractor->extract($query));
    }

    public function test_returns_null_for_a_raw_where(): void
    {
        $query = $this->builder()->whereRaw('length > ?', [10]);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_subquery_where(): void
    {
        $query = $this->builder()->whereIn('album_id', function ($q) {
            $q->select('id')->from('albums');
        });

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_extracts_integer_in_raw_columns(): void
    {
        $query = $this->builder()->whereIntegerInRaw('id', [1, 2, 3]);

        $this->assertSame(['id'], $this->extractor->extract($query));
    }

    public function test_extracts_integer_not_in_raw_columns(): void
    {
        $query = $this->builder()->whereIntegerNotInRaw('id', [1, 2, 3]);

        $this->assertSame(['id'], $this->extractor->extract($query));
    }

    public function test_returns_null_for_a_raw_order(): void
    {
        $query = $this->builder()->orderByRaw('rand()');

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_a_query_with_no_predicate_returns_an_empty_list(): void
    {
        $this->assertSame([], $this->extractor->extract($this->builder()));
    }

    public function test_extracts_between_columns(): void
    {
        $query = $this->builder()->whereBetween('length', [1, 10]);

        $this->assertSame(['length'], $this->extractor->extract($query));
    }

    public function test_extracts_not_between_columns(): void
    {
        $query = $this->builder()->whereNotBetween('length', [1, 10]);

        $this->assertSame(['length'], $this->extractor->extract($query));
    }

    public function test_returns_null_for_a_column_comparison(): void
    {
        $query = $this->builder()->whereColumn('starts_at', '<', 'ends_at');

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_an_exists_where(): void
    {
        $query = $this->builder()->whereExists(function ($q) {
            $q->select('id')->from('albums');
        });

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_between_columns(): void
    {
        $query = $this->builder()->whereBetweenColumns('length', ['min_length', 'max_length']);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_json_boolean_where(): void
    {
        $query = $this->builder()->where('data->flag', true);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_fulltext_where(): void
    {
        $query = $this->builder()->whereFullText(['title'], 'needle');

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_when_nested_where_contains_a_raw_clause(): void
    {
        $query = $this->builder()->where(function ($q) {
            $q->where('album_id', 1)->orWhereRaw('artist_id = ?', [2]);
        });

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_basic_where_comparing_to_another_column_via_expression(): void
    {
        $query = $this->builder()->where('updated_at', '>', DB::raw('created_at'));

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_between_where_with_expression_bounds(): void
    {
        $query = $this->builder()->whereBetween('length', [DB::raw('min_length'), DB::raw('max_length')]);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_between_where_with_one_expression_bound(): void
    {
        $query = $this->builder()->whereBetween('length', [1, DB::raw('max_length')]);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_json_path_where(): void
    {
        $query = $this->builder()->where('data->flag', 'x');

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_json_path_in_where(): void
    {
        $query = $this->builder()->whereIn('data->flag', [1, 2]);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_json_path_null_where(): void
    {
        $query = $this->builder()->whereNull('data->flag');

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_json_path_order(): void
    {
        $query = $this->builder()->orderBy('data->flag');

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_returns_null_for_a_json_path_containing_a_dot(): void
    {
        $query = $this->builder()->where('data->a.b', 1);

        $this->assertNull($this->extractor->extract($query));
    }

    public function test_extracted_column_names_are_always_strings(): void
    {
        $query = $this->builder()->where('123', 1);

        $columns = $this->extractor->extract($query);

        $this->assertSame(['123'], $columns);
        $this->assertIsString($columns[0]);
    }
}
