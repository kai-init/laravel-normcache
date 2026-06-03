<?php

namespace NormCache\Tests\Unit;

use Illuminate\Contracts\Database\Query\Expression as ExpressionContract;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Grammar;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use NormCache\Planning\QueryAnalyzer;
use PHPUnit\Framework\TestCase;

class QueryAnalyzerTest extends TestCase
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

        $resolvedColumns = QueryAnalyzer::resolveSelectedColumns($this->makeBaseQuery(columns: $columns), null);

        $this->assertSame($columns, $resolvedColumns);
        $this->assertTrue(QueryAnalyzer::hasCalculatedColumns($resolvedColumns));
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
