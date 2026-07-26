<?php

namespace NormCache\Tests\Integration;

use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;

final class RedisProtocolTest extends TestCase
{
    public function test_canonical_publication_is_lease_and_state_guarded_and_reads_atomically(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->membership($table, '0', 'u', 'query');
        $buildKey = $keys->membershipBuild($table, '0', 'u', 'query');
        $token = str_repeat('a', 32);
        $wakeKey = $keys->wake($table, 'm', 'query', $token);
        $rowKey = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->setNxEx($buildKey, $token, 5));
        $this->assertTrue($store->publishCanonical(
            versionKey: $versionKey,
            generationKey: $generationKey,
            membershipKey: $membershipKey,
            rows: [$rowKey => 'row-payload'],
            expectedVersion: '0',
            expectedGeneration: '0',
            membershipPayload: '{"f":4,"ep":"0","g":"0","ids":["i:1"],"vec":[]}',
            membershipTtl: 60,
            rowTtl: 3600,
            buildingKey: $buildKey,
            wakeKey: $wakeKey,
            token: $token,
            wakeTtl: 11,
        ));

        $result = $store->fetchCanonical(
            versionKey: $versionKey,
            generationKey: $generationKey,
            tablePrefix: $keys->tablePrefix($table),
            namespace: 'u',
            queryHash: 'query',
            maxMembershipBytes: 1_048_576,
            maxMembershipRows: 1000,
        );

        $this->assertSame('hit', $result[0]);
        $this->assertSame('0', $result[1]);
        $this->assertSame('0', $result[2]);

        // The script resolves the key from ver, so a bump points it at an unwritten key.
        $store->increment($versionKey);
        $this->assertSame('miss', $store->fetchCanonical(
            $versionKey,
            $generationKey,
            $keys->tablePrefix($table),
            'u',
            'query',
            1_048_576,
            1000,
        )[0]);
    }

    public function test_expired_owner_cannot_publish_or_release_a_replacement_lease(): void
    {
        $store = app(RedisStore::class);
        $key = 'test:{nc4:x:lease}:result:u';
        $build = 'test:{nc4:x:lease}:build';
        $wake = 'test:{nc4:x:lease}:wake:' . str_repeat('a', 32);

        $store->setRaw($build, str_repeat('b', 32), 10);

        $this->assertFalse($store->storeVersionedPayload(
            [$key => 'stale'],
            60,
            [],
            [],
            $build,
            $wake,
            str_repeat('a', 32),
            11,
        ));
        $this->assertNull($store->getRaw($key));
        $this->assertSame(str_repeat('b', 32), $store->getRaw($build));
    }

    public function test_repair_publication_is_guard_and_lease_protected(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $repair = $keys->repairBuild($table, 'batch');
        $token = str_repeat('c', 32);
        $wake = $keys->repairWake($table, 'batch', $token);
        $guard = $keys->guard($table, 'i:1');
        $row = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->setNxEx($repair, $token, 5));
        $this->assertTrue($store->publishRepair(
            $keys->version($table),
            $keys->generation($table),
            [$guard => '0'],
            [$row => 'repaired'],
            '0',
            '0',
            3600,
            $repair,
            $wake,
            $token,
            11,
        ));
        $this->assertSame('repaired', $store->getRaw($row));
    }
}
