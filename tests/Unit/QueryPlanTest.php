<?php

namespace NormCache\Tests\Unit;

use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlanTest extends UnitTestCase
{
    public function test_full_result_has_no_row_generation(): void
    {
        $table = $this->table();
        $plan = QueryPlan::result($table, [$table]);

        $this->assertSame(QueryPlan::RESULT, $plan->route);
        $this->assertTrue($plan->isResult());
        $this->assertNull($plan->primaryKey);
        $this->assertFalse($plan->usesGeneration());
    }

    public function test_direct_primary_key_is_a_normalized_strategy_with_a_token(): void
    {
        $table = $this->table();
        $plan = QueryPlan::directPrimaryKey(
            $table,
            [$table],
            $this->primaryKey(),
            'i:1',
            'default',
            'deleted_at',
        );

        $this->assertSame(QueryPlan::DIRECT_PK, $plan->route);
        $this->assertTrue($plan->isDirectPrimaryKey());
        $this->assertSame('i:1', $plan->primaryKeyToken);
        $this->assertSame('default', $plan->softDeleteMode);
        $this->assertSame('deleted_at', $plan->deletedAtColumn);
        $this->assertTrue($plan->usesGeneration());
    }

    public function test_canonical_query_is_a_normalized_strategy_without_a_token(): void
    {
        $table = $this->table();
        $plan = QueryPlan::canonical($table, [$table], $this->primaryKey());

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
        $this->assertTrue($plan->isCanonical());
        $this->assertNull($plan->primaryKeyToken);
        $this->assertTrue($plan->usesGeneration());
    }

    private function table(): TableIdentity
    {
        return TableIdentity::fromParts('sqlite', 'testing', '/tmp/test.sqlite', '', '', 'posts');
    }

    private function primaryKey(): PrimaryKeyMetadata
    {
        return new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);
    }
}
