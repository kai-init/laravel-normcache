<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\TableIdentity;

final class CacheKeyBuilderTest extends UnitTestCase
{
    public function test_table_local_keys_share_one_hash_tag(): void
    {
        $table = TableIdentity::fromParts('mysql', 'main', 'app', 'app', '', 'posts');
        $keys = new CacheKeyBuilder('app:');

        $expectedTag = '{nc:t:' . $table->hash . '}';

        $this->assertSame('app:' . $expectedTag . ':ver', $keys->version($table));
        $this->assertSame('app:' . $expectedTag . ':gen', $keys->generation($table));
        $this->assertStringContainsString($expectedTag, $keys->queryEntry($table, '12', 'u', 'abc'));
        $this->assertStringContainsString($expectedTag, $keys->row($table, '4', 'i:42'));
        $this->assertStringContainsString($expectedTag, $keys->repairBuild($table, '4', 'batch'));
        $this->assertStringContainsString($expectedTag, $keys->repairWake($table, '4', 'batch', 'token'));
    }

    public function test_global_tag_and_query_group_keys_have_independent_groups(): void
    {
        $keys = new CacheKeyBuilder('app:');

        $this->assertSame('app:{ncm}:epoch', $keys->epoch());
        $this->assertSame('app:{ncm}:schema-epoch', $keys->schemaEpoch());
        $this->assertSame(
            'app:{ncm:c:' . hash('xxh128', 'mysql') . '}:schema:v4',
            $keys->schema('mysql', '4'),
        );
        $this->assertSame('app:{nc:g:taghash}:ver', $keys->tagVersion('taghash'));
        $this->assertSame(
            'app:{nc:x:queryhash}:q:gabc',
            $keys->queryGroupEntry('queryhash', 'gabc'),
        );
    }

    public function test_unique_tables_do_not_retain_process_lifetime_state(): void
    {
        $keys = new CacheKeyBuilder('app:');
        $before = memory_get_usage(false);

        for ($index = 0; $index < 50_000; $index++) {
            $keys->tablePrefix(TableIdentity::fromParts(
                'mysql',
                'tenant',
                "tenant_{$index}",
                "tenant_{$index}",
                '',
                'posts',
            ));
        }

        $this->assertLessThan(2 * 1_024 * 1_024, memory_get_usage(false) - $before);
    }
}
