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

        $expectedTag = '{nc4:t:' . $table->hash . '}';

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
            'app:{nc4:t:' . $table->hash . '}:repair:batch:wake:token',
            $keys->repairWake($table, 'batch', 'token'),
        );
    }

    public function test_global_tag_and_query_group_keys_have_independent_groups(): void
    {
        $keys = new CacheKeyBuilder('app:');

        $this->assertSame('app:{nc4m}:epoch', $keys->epoch());
        $this->assertSame('app:{nc4:g:taghash}:ver', $keys->tagVersion('taghash'));
        $this->assertSame(
            'app:{nc4:x:queryhash}:result:gabc',
            $keys->queryGroupResult('queryhash', 'gabc'),
        );
    }
}
