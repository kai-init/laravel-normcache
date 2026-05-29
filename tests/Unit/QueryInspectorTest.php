<?php

namespace NormCache\Tests\Unit;

use Illuminate\Contracts\Database\Query\Expression as ExpressionContract;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Grammar;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use NormCache\Support\QueryInspector;
use PHPUnit\Framework\TestCase;

class QueryInspectorTest extends TestCase
{
    public function test_alias_expression_columns_do_not_crash_inspection(): void
    {
        $columns = [new class implements ExpressionContract
        {
            public function getValue(Grammar $grammar): string
            {
                return 'users.id as "user_id"';
            }
        }];

        $resolvedColumns = QueryInspector::resolveSelectedColumns($this->makeBaseQuery(columns: $columns), null);

        $this->assertSame($columns, $resolvedColumns);
        $this->assertTrue(QueryInspector::hasCalculatedColumns($resolvedColumns));
    }

    public function test_is_cacheable_uses_fast_boolean_path_for_simple_queries(): void
    {
        $this->assertTrue(QueryInspector::isCacheable($this->makeBaseQuery(null), 'authors'));
    }

    public function test_dependency_bypasses_are_separate_from_structural_cacheability(): void
    {
        $query = $this->makeBaseQuery(null);
        $query->orders = [['type' => 'Raw', 'sql' => 'CASE WHEN active THEN 0 ELSE 1 END']];

        $this->assertFalse(QueryInspector::isCacheable($query, 'authors'));
        $this->assertTrue(QueryInspector::isStructurallyCacheable($query, 'authors'));
        $this->assertTrue(QueryInspector::hasDependencyBypass($query));
    }

    public function test_structural_cacheability_rejects_normalization_reasons(): void
    {
        $query = $this->makeBaseQuery(null);
        $query->groups = ['name'];

        $this->assertFalse(QueryInspector::isCacheable($query, 'authors'));
        $this->assertFalse(QueryInspector::isStructurallyCacheable($query, 'authors'));
    }

    public function test_is_cacheable_rejects_calculated_columns(): void
    {
        $query = $this->makeBaseQuery(['id', '1 + 1 as computed']);

        $this->assertFalse(QueryInspector::isCacheable($query, 'authors', $query->columns));
    }

    /**
     * @param  array<int, mixed>|null  $columns
     */
    private function makeBaseQuery(?array $columns, string $from = 'authors'): Builder
    {
        $query = new Builder(
            connection: $this->createStub(ConnectionInterface::class),
            grammar: $this->createStub(\Illuminate\Database\Query\Grammars\Grammar::class),
            processor: $this->createStub(Processor::class),
        );

        $query->columns = $columns;
        $query->from = $from;

        return $query;
    }
}
