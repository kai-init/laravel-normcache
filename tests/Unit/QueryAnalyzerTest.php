<?php

namespace NormCache\Tests\Unit;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Query\Processors\Processor;
use NormCache\Planning\BypassReasons;
use NormCache\Planning\QueryAnalyzer;
use NormCache\Values\QueryInspection;
use PHPUnit\Framework\TestCase;

class QueryAnalyzerTest extends TestCase
{
    public function test_simple_query_has_no_flags(): void
    {
        $analyzer = new QueryAnalyzer;
        $query = $this->makeBaseQuery();
        $inspection = $analyzer->inspect($query, 'authors', null);

        $this->assertSame(0, $inspection->flags);
        $this->assertSame(0, $analyzer->flags($query, 'authors', null));
        $this->assertTrue($inspection->dependencies->hasNoDependencies());
    }

    public function test_nested_wheres_are_scanned_once_for_raw_and_exists_flags(): void
    {
        $nested = $this->makeBaseQuery();
        $nested->wheres = [
            ['type' => 'raw', 'sql' => 'LOWER(name) = ?'],
            ['type' => 'Exists', 'query' => $this->makeBaseQuery()],
        ];

        $query = $this->makeBaseQuery();
        $query->wheres = [['type' => 'Nested', 'query' => $nested]];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', null);

        $this->assertTrue($inspection->has(QueryInspection::RAW_WHERE));
        $this->assertTrue($inspection->has(QueryInspection::EXISTS_WHERE));
        $this->assertFalse($inspection->has(QueryInspection::SUBQUERY_WHERE));
        $this->assertTrue($inspection->hasDependencyBypass());
    }

    public function test_notexists_where_type_sets_exists_where_flag(): void
    {
        $query = $this->makeBaseQuery();
        $query->wheres = [['type' => 'NotExists', 'query' => $this->makeBaseQuery()]];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', null);

        $this->assertTrue($inspection->has(QueryInspection::EXISTS_WHERE));
        $this->assertFalse($inspection->has(QueryInspection::SUBQUERY_WHERE));
        $this->assertFalse($inspection->hasDependencyBypass());
    }

    public function test_sub_where_type_sets_subquery_where_flag_not_exists_where(): void
    {
        $query = $this->makeBaseQuery();
        $query->wheres = [['type' => 'Sub', 'query' => $this->makeBaseQuery()]];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', null);

        $this->assertTrue($inspection->has(QueryInspection::SUBQUERY_WHERE));
        $this->assertFalse($inspection->has(QueryInspection::EXISTS_WHERE));
        $this->assertFalse($inspection->hasDependencyBypass());
    }

    public function test_structural_flags_map_to_existing_reason_strings(): void
    {
        $query = $this->makeBaseQuery(['id', '1 + 1 as computed'], 'other_authors');
        $query->joins = [(object) ['table' => 'countries as c']];
        $query->groups = ['id'];
        $query->havings = [['type' => 'Basic']];
        $query->unions = [['query' => $this->makeBaseQuery()]];
        $query->aggregate = ['function' => 'count', 'columns' => ['*']];
        $query->distinct = true;
        $query->lock = true;
        $query->orders = [['type' => 'Raw']];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', $query->columns);

        $this->assertSame(
            [
                'dependency' => ['raw ORDER expression'],
                'normalization' => [
                    'non-standard FROM (subquery or raw expression)',
                    'JOIN clauses',
                    'GROUP BY',
                    'HAVING',
                    'UNION',
                    'aggregate function (count/sum/etc.)',
                    'DISTINCT',
                    'calculated or raw SELECT expressions',
                ],
                'safety' => ['query lock (SELECT FOR UPDATE)'],
            ],
            BypassReasons::fromInspection($inspection),
        );
        $this->assertSame(['authors', 'countries'], (new QueryAnalyzer)->extractTables($query, 'authors'));
    }

    public function test_primary_keys_are_extracted_without_reason_generation(): void
    {
        $query = $this->makeBaseQuery();
        $query->wheres = [[
            'type' => 'In',
            'column' => 'authors.id',
            'values' => [3, 1, 2],
        ]];

        $inspection = (new QueryAnalyzer)->inspect(
            $query,
            'authors',
            null,
            ['id', 'authors.id'],
        );

        $this->assertSame([1, 2, 3], $inspection->primaryKeys);
    }

    public function test_primary_keys_allow_the_model_soft_delete_constraint(): void
    {
        $query = $this->makeBaseQuery(from: 'posts');
        $query->wheres = [
            ['type' => 'Null', 'column' => 'posts.deleted_at', 'boolean' => 'and'],
            ['type' => 'In', 'column' => 'posts.id', 'values' => [3, 1, 2]],
        ];

        $inspection = (new QueryAnalyzer)->inspect(
            $query,
            'posts',
            null,
            ['id', 'posts.id'],
            softDeleteScopeColumn: 'posts.deleted_at',
        );

        $this->assertSame([1, 2, 3], $inspection->primaryKeys);
    }

    public function test_primary_keys_do_not_ignore_an_arbitrary_null_constraint(): void
    {
        $query = $this->makeBaseQuery(from: 'posts');
        $query->wheres = [
            ['type' => 'Null', 'column' => 'posts.published_at', 'boolean' => 'and'],
            ['type' => 'In', 'column' => 'posts.id', 'values' => [3, 1, 2]],
        ];

        $inspection = (new QueryAnalyzer)->inspect(
            $query,
            'posts',
            null,
            ['id', 'posts.id'],
            softDeleteScopeColumn: 'posts.deleted_at',
        );

        $this->assertNull($inspection->primaryKeys);
    }

    public function test_expression_values_are_subquery_bypasses(): void
    {
        $expression = $this->createStub(Expression::class);
        $query = $this->makeBaseQuery();
        $query->wheres = [[
            'type' => 'In',
            'column' => 'id',
            'values' => [$expression],
        ]];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', null, ['id']);

        $this->assertTrue($inspection->has(QueryInspection::SUBQUERY_WHERE));
        $this->assertNull($inspection->primaryKeys);
    }

    public function test_exists_and_subquery_flags_do_not_bypass_dependency_inference(): void
    {
        $exists = new QueryInspection(flags: QueryInspection::EXISTS_WHERE);
        $subquery = new QueryInspection(flags: QueryInspection::SUBQUERY_WHERE);

        $this->assertFalse($exists->hasDependencyBypass());
        $this->assertFalse($subquery->hasDependencyBypass());
        $this->assertSame([], BypassReasons::fromInspection($exists));
    }

    public function test_raw_where_still_bypasses_when_combined_with_exists(): void
    {
        $inspection = new QueryInspection(flags: QueryInspection::EXISTS_WHERE | QueryInspection::RAW_WHERE);

        $this->assertTrue($inspection->hasDependencyBypass());
        $this->assertContains('raw WHERE expression', BypassReasons::fromInspection($inspection)['dependency']);
    }

    public function test_direct_primary_key_inspection_allows_harmless_single_row_ordering(): void
    {
        $query = $this->makeBaseQuery();
        $query->wheres = [[
            'type' => 'Basic',
            'column' => 'id',
            'operator' => '=',
            'value' => 1,
        ]];
        $query->orders = [['type' => 'Raw', 'sql' => 'CASE WHEN id = 1 THEN 0 END']];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', null, ['id', 'authors.id']);

        $this->assertSame([1], $inspection->primaryKeys);
        $this->assertSame(0, $inspection->normalizationFlags());
        $this->assertFalse($inspection->hasSafetyBypass());
    }

    public function test_direct_primary_key_inspection_rejects_structural_query_shapes(): void
    {
        $query = $this->makeBaseQuery();
        $query->wheres = [[
            'type' => 'Basic',
            'column' => 'id',
            'operator' => '=',
            'value' => 1,
        ]];
        $query->groups = ['id'];

        $inspection = (new QueryAnalyzer)->inspect($query, 'authors', null, ['id', 'authors.id']);

        $this->assertNotSame(0, $inspection->normalizationFlags());
    }

    public function test_query_dependencies_include_nested_where_and_union_tables(): void
    {
        $exists = $this->makeBaseQuery(from: 'posts as p');
        $union = $this->makeBaseQuery(from: 'archived_authors');
        $query = $this->makeBaseQuery();
        $query->wheres = [['type' => 'Exists', 'query' => $exists]];
        $query->unions = [['query' => $union]];

        $dependencies = (new QueryAnalyzer)->inferQueryDependencies($query, 'testing', 'authors');

        $this->assertTrue($dependencies->safe);
        $this->assertSame(['testing:posts', 'testing:archived_authors'], $dependencies->tables);
    }

    public function test_query_dependencies_reject_opaque_join_sources(): void
    {
        $query = $this->makeBaseQuery();
        $query->joins = [(object) ['table' => $this->createStub(Expression::class), 'wheres' => []]];

        $dependencies = (new QueryAnalyzer)->inferQueryDependencies($query, 'testing', 'authors');

        $this->assertFalse($dependencies->safe);
        $this->assertSame(['joined subquery dependency could not be inferred'], $dependencies->reasons);
    }

    private function makeBaseQuery(?array $columns = null, string $from = 'authors'): Builder
    {
        $query = new Builder(
            connection: $this->createStub(ConnectionInterface::class),
            grammar: $this->createStub(Grammar::class),
            processor: $this->createStub(Processor::class),
        );

        $query->columns = $columns;
        $query->from = $from;

        return $query;
    }
}
