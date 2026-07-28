<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\QueryIdentity;
use NormCache\Tests\UnitTestCase;

final class QueryIdentityTest extends UnitTestCase
{
    public function test_identity_is_typed_ordered_and_full_width(): void
    {
        $identity = new QueryIdentity;

        $integer = $identity->hash(
            route: 'result',
            rootHash: 'root',
            dependencyHashes: ['b', 'a', 'a'],
            sql: 'select * from posts where id = ?',
            bindings: [42],
            namespace: 'u',
            operation: 'select',
        );
        $string = $identity->hash(
            route: 'result',
            rootHash: 'root',
            dependencyHashes: ['a', 'b'],
            sql: 'select * from posts where id = ?',
            bindings: ['42'],
            namespace: 'u',
            operation: 'select',
        );

        $this->assertSame(32, strlen($integer));
        $this->assertNotSame($integer, $string);
        $this->assertSame(
            $integer,
            $identity->hash('result', 'root', ['a', 'b'], 'select * from posts where id = ?', [42], 'u', 'select'),
        );
    }

    public function test_tags_use_a_separate_domain_and_validate_input(): void
    {
        $identity = new QueryIdentity;

        $this->assertSame(32, strlen($identity->tagHash('homepage')));
        $this->assertSame('g' . $identity->tagHash('homepage'), $identity->namespace('homepage'));
        $this->assertSame('u', $identity->namespace(null));

        $this->expectException(\InvalidArgumentException::class);
        $identity->tagHash('');
    }

    public function test_unique_tags_do_not_retain_process_lifetime_state(): void
    {
        $identity = new QueryIdentity;
        $before = memory_get_usage(false);

        for ($index = 0; $index < 50_000; $index++) {
            $identity->namespace("tenant-{$index}");
        }

        $this->assertLessThan(2 * 1_024 * 1_024, memory_get_usage(false) - $before);
    }
}
