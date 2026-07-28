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

        $this->assertSame('bfad48eba036b467ac6c47dd907a40f5', $hash(QueryPlan::CANONICAL));
        $this->assertSame('6eaf3005c2eec1aa917697e1c33736ef', $hash(QueryPlan::RESULT));
        $this->assertSame('3de633029da49012b990968ca40ff077', $hash(QueryPlan::QUERY_GROUP));
        $this->assertSame('d9adbde04aeb0db232db1085af480b23', $hash(QueryPlan::DIRECT_PK));
    }

    public function test_tag_repair_and_table_digests_are_stable(): void
    {
        $identity = new QueryIdentity;

        $this->assertSame('b3838353748f6c156f5592a9b3ea18ce', $identity->tagHash('homepage'));
        $this->assertSame(
            '8235f0f91f60f29871723f520313747d',
            $identity->repairHash('roothash', '7', ['i:9', 'i:2']),
        );
        $this->assertSame(
            '3705949d1b712ff0cf2bf4bf8aead17c',
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
