<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\QueryInspector;
use PHPUnit\Framework\TestCase;
use Illuminate\Contracts\Database\Query\Expression as ExpressionContract;
use Illuminate\Database\Grammar;

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

    /**
     * @param  array<int, mixed>|null  $columns
     */
    private function makeBaseQuery(?array $columns): \Illuminate\Database\Query\Builder
    {
        $query = new \Illuminate\Database\Query\Builder(
            connection: $this->createStub(\Illuminate\Database\ConnectionInterface::class),
            grammar: $this->createStub(\Illuminate\Database\Query\Grammars\Grammar::class),
            processor: $this->createStub(\Illuminate\Database\Query\Processors\Processor::class),
        );

        $query->columns = $columns;

        return $query;
    }
}
