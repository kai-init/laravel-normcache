<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\QueryIdentity;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class IdentityStabilityTest extends UnitTestCase
{
    public function test_field_encoding_cannot_collide_across_different_field_sets(): void
    {
        $this->assertNotSame(
            TableIdentity::encodeFields(['a:b', 'c']),
            TableIdentity::encodeFields(['a', 'b:c']),
        );
        $this->assertSame('2:ab0:1:c', TableIdentity::encodeFields(['ab', '', 'c']));
    }

    public function test_query_hash_is_stable_per_route_for_prepared_bindings(): void
    {
        $identity = new QueryIdentity;

        $hash = static fn(string $route): string => $identity->hash(
            route: $route,
            rootHash: 'roothash',
            dependencyHashes: ['deps-b', 'deps-a'],
            sql: 'select * from "posts" where "id" = ?',
            bindings: [42, 'x', null, 1, 1.5],
            namespace: 'u',
            operation: 'select',
        );

        $this->assertSame('548acc7208c045aac641546e36bdbffc', $hash(QueryPlan::CANONICAL));
        $this->assertSame('f982b6a4ee8b0a2c9854ce4dffe05908', $hash(QueryPlan::RESULT));
        $this->assertSame('09bf37953b8677842e6d58ac178f35eb', $hash(QueryPlan::QUERY_GROUP));
        $this->assertSame('1427a690b77f0404a4ecbc02f48a3dbc', $hash(QueryPlan::DIRECT_PK));
    }

    public function test_tag_and_table_digests_are_stable(): void
    {
        $identity = new QueryIdentity;

        $this->assertSame('70cf626fa4c84d4ae1d3931451bf301c', $identity->tagHash('homepage'));
        $this->assertSame(
            '06a3bba515102093afe9de576d942169',
            TableIdentity::fromParts(
                driver: 'mysql',
                connection: 'conn',
                database: 'db',
                schema: 'sch',
                prefix: 'pre',
                table: 'posts',
            )->hash,
        );
    }
}
