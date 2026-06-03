<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Query\Processors\Processor;
use NormCache\Planning\BypassReasons;
use PHPUnit\Framework\TestCase;

class BypassReasonsTest extends TestCase
{
    public function test_simple_query_has_no_bypass_reasons(): void
    {
        $this->assertSame([], BypassReasons::forQuery($this->makeBaseQuery(null), 'authors'));
    }

    public function test_raw_order_is_reported_as_dependency_bypass_reason(): void
    {
        $query = $this->makeBaseQuery(null);
        $query->orders = [['type' => 'Raw', 'sql' => 'CASE WHEN active THEN 0 ELSE 1 END']];

        $this->assertSame(
            ['dependency' => ['raw ORDER expression']],
            BypassReasons::forQuery($query, 'authors')
        );
    }

    public function test_raw_where_is_reported_as_dependency_bypass_reason(): void
    {
        $query = $this->makeBaseQuery(null);
        $query->wheres = [['type' => 'Raw', 'sql' => 'LOWER(name) = ?', 'boolean' => 'and']];

        $this->assertSame(
            ['dependency' => ['raw WHERE expression']],
            BypassReasons::forQuery($query, 'authors')
        );
    }

    public function test_group_by_is_reported_as_normalization_bypass_reason(): void
    {
        $query = $this->makeBaseQuery(null);
        $query->groups = ['name'];

        $this->assertSame(
            ['normalization' => ['GROUP BY']],
            BypassReasons::forQuery($query, 'authors')
        );
    }

    public function test_calculated_columns_are_reported_as_normalization_bypass_reason(): void
    {
        $query = $this->makeBaseQuery(['id', '1 + 1 as computed']);

        $this->assertSame(
            ['normalization' => ['calculated or raw SELECT expressions']],
            BypassReasons::forQuery($query, 'authors', $query->columns)
        );
    }

    /**
     * @param  array<int, mixed>|null  $columns
     */
    private function makeBaseQuery(?array $columns, string $from = 'authors'): Builder
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
