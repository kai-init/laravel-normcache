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
        $this->assertStringContainsString($expectedTag, $keys->membership($table, '12', 'u', 'abc'));
        $this->assertStringContainsString($expectedTag, $keys->result($table, '12', 'u', 'abc'));
        $this->assertStringContainsString($expectedTag, $keys->row($table, '4', 'i:42'));
    }

    public function test_repair_wake_key_places_wake_after_the_repair_batch_segment(): void
    {
        $table = TableIdentity::fromParts('mysql', 'main', 'app', 'app', '', 'posts');
        $keys = new CacheKeyBuilder('app:');

        $this->assertSame(
            'app:{nc:t:' . $table->hash . '}:repair:batch:wake:token',
            $keys->repairWake($table, 'batch', 'token'),
        );
    }

    public function test_global_tag_and_query_group_keys_have_independent_groups(): void
    {
        $keys = new CacheKeyBuilder('app:');

        $this->assertSame('app:{ncm}:epoch', $keys->epoch());
        $this->assertSame('app:{nc:g:taghash}:ver', $keys->tagVersion('taghash'));
        $this->assertSame(
            'app:{nc:x:queryhash}:result:gabc',
            $keys->queryGroupResult('queryhash', 'gabc'),
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
