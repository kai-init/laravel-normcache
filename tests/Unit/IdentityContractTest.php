<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\QueryIdentity;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class IdentityContractTest extends UnitTestCase
{
    public function test_field_encoding_cannot_collide_across_different_field_sets(): void
    {
        $this->assertNotSame(
            TableIdentity::encodeFields(['a:b', 'c']),
            TableIdentity::encodeFields(['a', 'b:c']),
        );
        $this->assertSame('2:ab0:1:c', TableIdentity::encodeFields(['ab', '', 'c']));
    }

    public function test_query_hash_is_stable_per_route(): void
    {
        $identity = new QueryIdentity;

        $hash = static fn(string $route): string => $identity->hash(
            route: $route,
            rootHash: 'roothash',
            dependencyHashes: ['deps-b', 'deps-a'],
            sql: 'select * from "posts" where "id" = ?',
            bindings: [42, 'x', null, true, 1.5],
            namespace: 'u',
            operation: 'select',
        );

        $this->assertSame('60465e1baf4064e424cf842786cb9789', $hash(QueryPlan::CANONICAL));
        $this->assertSame('3b70a6d0452413dbba57400d1bb63a46', $hash(QueryPlan::RESULT));
        $this->assertSame('10fe47d045deffca1f538f9d4b28d46b', $hash(QueryPlan::QUERY_GROUP));
        $this->assertSame('e42fc975f9bae0ce6a5d4eced9d1d21e', $hash(QueryPlan::DIRECT_PK));
    }

    public function test_tag_repair_and_table_digests_are_stable(): void
    {
        $identity = new QueryIdentity;

        $this->assertSame('70cf626fa4c84d4ae1d3931451bf301c', $identity->tagHash('homepage'));
        $this->assertSame(
            'ee352ad782d5558a44ba6e7a01230b9d',
            $identity->repairHash('roothash', '7', ['i:9', 'i:2']),
        );
        $this->assertSame(
            '45485a5ec5cba05e1bd85f13cee0e669',
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
