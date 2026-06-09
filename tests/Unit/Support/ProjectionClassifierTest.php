<?php

namespace NormCache\Tests\Unit\Support;

use Illuminate\Contracts\Database\Query\Expression as ExpressionContract;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Grammar;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar as QueryGrammar;
use Illuminate\Database\Query\Processors\Processor;
use NormCache\Support\ProjectionClassifier;
use PHPUnit\Framework\TestCase;

class ProjectionClassifierTest extends TestCase
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

        $resolvedColumns = ProjectionClassifier::resolve($this->makeBaseQuery(columns: $columns), null);

        $this->assertSame($columns, $resolvedColumns);
        $this->assertTrue(ProjectionClassifier::hasCalculatedColumns($resolvedColumns));
    }

    public function test_is_exact_full_model_projection(): void
    {
        $this->assertTrue(ProjectionClassifier::isExactFullModelProjection(null, 'authors'));
        $this->assertTrue(ProjectionClassifier::isExactFullModelProjection(['*'], 'authors'));
        $this->assertTrue(ProjectionClassifier::isExactFullModelProjection(['authors.*'], 'authors'));
        $this->assertFalse(ProjectionClassifier::isExactFullModelProjection(['id'], 'authors'));
        $this->assertFalse(ProjectionClassifier::isExactFullModelProjection(['authors.id'], 'authors'));
    }

    public function test_contains_wildcard(): void
    {
        $this->assertFalse(ProjectionClassifier::containsWildcard(null));
        $this->assertTrue(ProjectionClassifier::containsWildcard(['*']));
        $this->assertTrue(ProjectionClassifier::containsWildcard(['authors.*']));
        $this->assertFalse(ProjectionClassifier::containsWildcard(['id']));
    }

    public function test_has_required_key(): void
    {
        $this->assertTrue(ProjectionClassifier::hasRequiredKey(['*'], 'authors', 'id'));
        $this->assertTrue(ProjectionClassifier::hasRequiredKey(['authors.*'], 'authors', 'id'));
        $this->assertTrue(ProjectionClassifier::hasRequiredKey(['id'], 'authors', 'id'));
        $this->assertTrue(ProjectionClassifier::hasRequiredKey(['authors.id'], 'authors', 'id'));
        $this->assertFalse(ProjectionClassifier::hasRequiredKey(['name'], 'authors', 'id'));
    }

    /**
     * @param  array<int, mixed>|null  $columns
     */
    private function makeBaseQuery(?array $columns, string $from = 'authors'): Builder
    {
        $query = new Builder(
            connection: $this->createStub(ConnectionInterface::class),
            grammar: $this->createStub(QueryGrammar::class),
            processor: $this->createStub(Processor::class),
        );

        $query->columns = $columns;
        $query->from = $from;

        return $query;
    }
}
