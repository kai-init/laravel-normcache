<?php

namespace NormCache\Tests\Unit;

use LogicException;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlanTest extends UnitTestCase
{
    public function test_query_group_plans_carry_no_row_state(): void
    {
        $table = $this->table();
        $plan = QueryPlan::queryGroup($table, [$table]);

        $this->assertSame(QueryPlan::QUERY_GROUP, $plan->route);
        $this->assertNull($plan->primaryKey);
        $this->assertNull($plan->primaryKeyToken);
        $this->assertNull($plan->softDeleteMode);
        $this->assertNull($plan->deletedAtColumn);
        $this->assertNull($plan->projectedColumns);
        $this->assertFalse($plan->materializeResult);
    }

    public function test_direct_primary_key_plans_carry_a_token_and_never_project(): void
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
        $this->assertSame('i:1', $plan->primaryKeyToken);
        $this->assertSame('default', $plan->softDeleteMode);
        $this->assertSame('deleted_at', $plan->deletedAtColumn);
        $this->assertNull($plan->projectedColumns);
        $this->assertFalse($plan->materializeResult);
    }

    public function test_canonical_plans_carry_no_token_or_projection(): void
    {
        $table = $this->table();
        $plan = QueryPlan::canonical($table, [$table], $this->primaryKey(), materializeResult: true);

        $this->assertSame(QueryPlan::CANONICAL, $plan->route);
        $this->assertNull($plan->primaryKeyToken);
        $this->assertNull($plan->projectedColumns);
        $this->assertNull($plan->softDeleteMode);
        $this->assertTrue($plan->materializeResult);
    }

    public function test_result_plans_never_materialize(): void
    {
        $table = $this->table();

        $this->assertFalse(QueryPlan::result($table, [$table])->materializeResult);
        $this->assertFalse(
            QueryPlan::projectedResult($table, [$table], $this->primaryKey(), ['id'])
                ->materializeResult,
        );
        $this->assertFalse(
            QueryPlan::projectedRow($table, [$table], $this->primaryKey(), 'i:1', ['id'], null, null)
                ->materializeResult,
        );
    }

    public function test_a_projected_result_converts_to_a_non_materializing_canonical_plan(): void
    {
        $table = $this->table();
        $primaryKey = $this->primaryKey();
        $fallback = QueryPlan::projectedResult($table, [$table], $primaryKey, ['id', 'title'])
            ->asCanonicalProjectionFallback();

        $this->assertSame(QueryPlan::CANONICAL, $fallback->route);
        $this->assertSame($primaryKey, $fallback->primaryKey);
        $this->assertNull($fallback->projectedColumns);
        $this->assertNull($fallback->primaryKeyToken);
        $this->assertFalse($fallback->materializeResult);
    }

    public function test_a_canonical_plan_converts_to_a_full_result_overlay(): void
    {
        $table = $this->table();
        $primaryKey = $this->primaryKey();
        $overlay = QueryPlan::canonical($table, [$table], $primaryKey, materializeResult: true)
            ->asFullResultOverlay();

        $this->assertSame(QueryPlan::RESULT, $overlay->route);
        $this->assertSame($primaryKey, $overlay->primaryKey);
        $this->assertNull($overlay->projectedColumns);
        $this->assertFalse($overlay->materializeResult);
    }

    public function test_a_row_backed_projection_has_no_canonical_projection_fallback(): void
    {
        $table = $this->table();
        $plan = QueryPlan::projectedRow(
            $table,
            [$table],
            $this->primaryKey(),
            'i:1',
            ['id'],
            null,
            null,
        );

        $this->expectException(LogicException::class);

        $plan->asCanonicalProjectionFallback();
    }

    public function test_an_unprojected_result_has_no_canonical_projection_fallback(): void
    {
        $table = $this->table();

        $this->expectException(LogicException::class);

        QueryPlan::result($table, [$table], $this->primaryKey())->asCanonicalProjectionFallback();
    }

    public function test_only_canonical_plans_convert_to_a_result_overlay(): void
    {
        $table = $this->table();

        $this->expectException(LogicException::class);

        QueryPlan::result($table, [$table])->asFullResultOverlay();
    }

    public function test_plan_predicates_express_route_capabilities(): void
    {
        $table = $this->table();
        $primaryKey = $this->primaryKey();
        $queryGroup = QueryPlan::queryGroup($table, [$table]);
        $direct = QueryPlan::directPrimaryKey(
            $table,
            [$table],
            $primaryKey,
            'i:1',
            null,
            null,
        );
        $canonical = QueryPlan::canonical(
            $table,
            [$table],
            $primaryKey,
            materializeResult: true,
        );
        $projectedResult = QueryPlan::projectedResult(
            $table,
            [$table],
            $primaryKey,
            ['id'],
        );
        $projectedRow = QueryPlan::projectedRow(
            $table,
            [$table],
            $primaryKey,
            'i:1',
            ['id'],
            null,
            null,
        );

        $this->assertTrue($queryGroup->isQueryGroup());
        $this->assertFalse($queryGroup->usesGeneration());
        $this->assertTrue($direct->isDirectPrimaryKey());
        $this->assertTrue($direct->usesGeneration());
        $this->assertTrue($canonical->isCanonical());
        $this->assertTrue($canonical->shouldMaterializeResult());
        $this->assertTrue($projectedResult->isResult());
        $this->assertTrue($projectedResult->supportsCanonicalProjectionFallback());
        $this->assertFalse($projectedResult->supportsRowFallback());
        $this->assertTrue($projectedRow->supportsRowFallback());
        $this->assertFalse($projectedRow->supportsCanonicalProjectionFallback());
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
