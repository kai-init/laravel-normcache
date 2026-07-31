<?php

namespace NormCache\Tests\Integration;

use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\ResultRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheState;
use Predis\Client;
use ReflectionProperty;

final class RecordingPredisClusterConnection extends PredisClusterConnection
{
    public int $pipelineCalls = 0;

    public int $directMgetCalls = 0;

    public function command($method, array $parameters = [])
    {
        if (strtolower((string) $method) === 'mget') {
            $this->directMgetCalls++;

            $keys = $parameters[0] ?? [];

            if (!is_array($keys)) {
                $keys = $parameters;
            }

            return array_map(
                static fn(string $key): string => "value:{$key}",
                $keys,
            );
        }

        return null;
    }

    public function pipeline(...$arguments)
    {
        $this->pipelineCalls++;

        return [];
    }
}

final class RedisProtocolTest extends TestCase
{
    public function test_predis_cluster_cross_slot_reads_use_one_pipeline(): void
    {
        $connection = new RecordingPredisClusterConnection(new Client);
        $store = new RedisStore('unused');
        $property = new ReflectionProperty($store, 'connection');
        $property->setValue($store, $connection);

        $store->mget([
            '{slot-a}:version',
            '{slot-b}:version',
        ]);

        $this->assertSame([
            'pipeline_calls' => 1,
            'direct_mget_calls' => 0,
        ], [
            'pipeline_calls' => $connection->pipelineCalls,
            'direct_mget_calls' => $connection->directMgetCalls,
        ]);
    }

    public function test_predis_cluster_same_slot_reads_use_direct_mget(): void
    {
        $connection = new RecordingPredisClusterConnection(new Client);
        $store = new RedisStore('unused');
        $property = new ReflectionProperty($store, 'connection');
        $property->setValue($store, $connection);

        $keys = ['{slot-a}:version', '{slot-a}:generation'];

        $this->assertSame([
            '{slot-a}:version' => 'value:{slot-a}:version',
            '{slot-a}:generation' => 'value:{slot-a}:generation',
        ], $store->mget($keys));
        $this->assertSame([
            'pipeline_calls' => 0,
            'direct_mget_calls' => 1,
        ], [
            'pipeline_calls' => $connection->pipelineCalls,
            'direct_mget_calls' => $connection->directMgetCalls,
        ]);
    }

    public function test_corrupt_result_read_does_not_delete_the_observed_payload(): void
    {
        $store = app(RedisStore::class);
        $key = 'test:{nc:x:corrupt-read}:payload';
        $store->setRaw($key, 'corrupt', 60);
        $state = new CacheState(
            key: $key,
            epoch: '0',
            version: '0',
            generation: '0',
            versions: [],
            tag: null,
            tagKey: null,
        );

        $result = app(ResultRepository::class)->read($state, 'corrupt');

        $this->assertSame('corrupt_payload', $result->reason);
        $this->assertSame('corrupt', $store->getRaw($key));
    }

    public function test_phpredis_raw_operations_preserve_the_shared_serializer(): void
    {
        $connection = Redis::connection('normcache-test');

        if (!$connection instanceof PhpRedisConnection) {
            $this->markTestSkipped('PhpRedis only.');
        }

        $client = $connection->client();
        $originalSerializer = $client->getOption(\Redis::OPT_SERIALIZER);
        $sharedKey = 'test:shared-serialized-value';
        $rawKey = 'test:raw-normcache-value';

        try {
            $client->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_PHP);
            $connection->set($sharedKey, ['value' => 123]);

            $store = app(RedisStore::class);
            $store->setRaw($rawKey, 'raw-value', 60);

            $this->assertSame('raw-value', $store->getRaw($rawKey));
            $this->assertSame(['value' => 123], $connection->get($sharedKey));
            $this->assertSame(
                \Redis::SERIALIZER_PHP,
                $client->getOption(\Redis::OPT_SERIALIZER),
            );
        } finally {
            $client->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_NONE);
            $connection->del($sharedKey, $rawKey);
            $client->setOption(\Redis::OPT_SERIALIZER, $originalSerializer);
        }
    }

    public function test_phpredis_lua_owner_comparison_works_with_shared_serializer_enabled(): void
    {
        $connection = Redis::connection('normcache-test');

        if (!$connection instanceof PhpRedisConnection) {
            $this->markTestSkipped('PhpRedis only.');
        }

        $client = $connection->client();
        $originalSerializer = $client->getOption(\Redis::OPT_SERIALIZER);
        $buildingKey = 'test:{nc:x:serializer}:build';
        $wakeKey = 'test:{nc:x:serializer}:wake';
        $token = str_repeat('a', 32);

        try {
            $client->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_PHP);
            $store = app(RedisStore::class);

            $this->assertTrue($store->setNxEx($buildingKey, $token, 60));
            $this->assertTrue($store->releaseBuilding($buildingKey, $wakeKey, $token));
            $this->assertNull($store->getRaw($buildingKey));
            $this->assertSame(
                \Redis::SERIALIZER_PHP,
                $client->getOption(\Redis::OPT_SERIALIZER),
            );
        } finally {
            $client->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_NONE);
            $connection->del($buildingKey, $wakeKey);
            $client->setOption(\Redis::OPT_SERIALIZER, $originalSerializer);
        }
    }

    public function test_release_building_wakes_each_configured_waiter(): void
    {
        $connection = Redis::connection('normcache-test');
        $store = new RedisStore('normcache-test', 3);
        $buildingKey = 'test:{nc:x:wake-count}:build';
        $wakeKey = 'test:{nc:x:wake-count}:wake';
        $token = str_repeat('a', 32);

        try {
            $this->assertTrue($store->setNxEx($buildingKey, $token, 60));
            $this->assertTrue($store->releaseBuilding($buildingKey, $wakeKey, $token));
            $this->assertSame(3, $connection->llen($wakeKey));
        } finally {
            $connection->del($buildingKey, $wakeKey);
        }
    }

    public function test_phpredis_raw_mode_is_applied_to_a_replacement_client(): void
    {
        $connection = Redis::connection('normcache-test');

        if (
            !$connection instanceof PhpRedisConnection
            || !$connection->client() instanceof \Redis
        ) {
            $this->markTestSkipped('Standalone PhpRedis only.');
        }

        $store = app(RedisStore::class);
        $store->getRaw('test:warm-store-connection');

        $property = new ReflectionProperty($connection, 'client');
        $originalClient = $connection->client();
        $replacement = new \Redis;
        $replacement->connect(
            (string) env('REDIS_HOST', '127.0.0.1'),
            (int) env('REDIS_PORT', 6379),
        );
        $replacement->select(15);
        $replacement->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_PHP);
        $key = 'test:replacement-client-value';

        try {
            $property->setValue($connection, $replacement);

            $store->setRaw($key, 'replacement-raw-value', 60);

            $this->assertSame('replacement-raw-value', $store->getRaw($key));
            $this->assertSame(
                \Redis::SERIALIZER_PHP,
                $replacement->getOption(\Redis::OPT_SERIALIZER),
            );
        } finally {
            $replacement->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_NONE);
            $replacement->del($key);
            $property->setValue($connection, $originalClient);
            $replacement->close();
        }
    }

    public function test_canonical_fast_path_publishes_rows_and_membership_together(): void
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
        )[0]);
    }

    public function test_canonical_publication_carries_the_result_overlay_under_the_same_guard(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->membership($table, '0', 'u', 'query');
        $buildKey = $keys->membershipBuild($table, '0', 'u', 'query');
        $token = str_repeat('e', 32);
        $wakeKey = $keys->wake($table, 'm', 'query', $token);
        $rowKey = $keys->row($table, '0', 'i:1');
        $overlayKey = $keys->result($table, '0', 'u', 'query');

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
            resultKey: $overlayKey,
            resultPayload: 'overlay-payload',
        ));

        // One publication, so readers find the overlay ahead of the membership it was built from.
        $this->assertSame(
            ['result', '0', 'overlay-payload'],
            array_slice((array) $store->fetchResultOrCanonical(
                versionKey: $versionKey,
                generationKey: $generationKey,
                tablePrefix: $keys->tablePrefix($table),
                namespace: 'u',
                resultQueryHash: 'query',
                canonicalQueryHash: 'query',
            ), 0, 3),
        );
        $this->assertSame('hit', $store->fetchCanonical(
            $versionKey,
            $generationKey,
            $keys->tablePrefix($table),
            'u',
            'query',
        )[0]);
        $this->assertSame(60, Redis::connection('normcache-test')->ttl($overlayKey));
    }

    public function test_canonical_publication_rejects_the_result_overlay_after_state_changes(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->membership($table, '0', 'u', 'guarded');
        $buildKey = $keys->membershipBuild($table, '0', 'u', 'guarded');
        $token = str_repeat('f', 32);
        $wakeKey = $keys->wake($table, 'm', 'guarded', $token);
        $rowKey = $keys->row($table, '0', 'i:1');
        $overlayKey = $keys->result($table, '0', 'u', 'guarded');

        $this->assertTrue($store->setNxEx($buildKey, $token, 5));
        $store->increment($generationKey);

        $this->assertFalse($store->publishCanonical(
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
            resultKey: $overlayKey,
            resultPayload: 'overlay-payload',
        ));

        $this->assertNull($store->getRaw($membershipKey));
        $this->assertNull($store->getRaw($rowKey));
        $this->assertNull($store->getRaw($overlayKey));
    }

    public function test_canonical_publication_rejects_membership_after_state_changes(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->membership($table, '0', 'u', 'changed');
        $buildKey = $keys->membershipBuild($table, '0', 'u', 'changed');
        $token = str_repeat('d', 32);
        $wakeKey = $keys->wake($table, 'm', 'changed', $token);
        $rowKey = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->setNxEx($buildKey, $token, 5));
        $store->increment($versionKey);

        $this->assertFalse($store->publishCanonical(
            $versionKey,
            $generationKey,
            $membershipKey,
            [$rowKey => 'row-payload'],
            '0',
            '0',
            '{"f":4,"ep":"0","g":"0","ids":["i:1"],"vec":[]}',
            60,
            3600,
            $buildKey,
            $wakeKey,
            $token,
            11,
        ));
        $this->assertNull($store->getRaw($membershipKey));
        $this->assertNull($store->getRaw($rowKey));
    }

    public function test_result_or_canonical_head_prefers_result_then_returns_membership(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $prefix = $keys->tablePrefix($table);
        $resultKey = $keys->result($table, '0', 'u', 'result-query');
        $membershipKey = $keys->membership($table, '0', 'u', 'canonical-query');
        $membership = '{"f":4,"ep":"0","g":"0","ids":["i:1"],"vec":[]}';

        $store->setRaw($membershipKey, $membership, 60);
        $store->setRaw($resultKey, 'result-payload', 60);

        $result = $store->fetchResultOrCanonical(
            $versionKey,
            $generationKey,
            $prefix,
            'u',
            'result-query',
            'canonical-query',
        );

        $this->assertSame(['result', '0', 'result-payload'], $result);

        $store->delete($resultKey);
        $canonical = $store->fetchResultOrCanonical(
            $versionKey,
            $generationKey,
            $prefix,
            'u',
            'result-query',
            'canonical-query',
        );

        $this->assertSame('membership', $canonical[0]);
        $this->assertSame('0', $canonical[1]);
        $this->assertSame('0', $canonical[2]);
        $this->assertSame($membership, $canonical[3]);

        $store->increment($versionKey);
        $this->assertSame('miss', $store->fetchResultOrCanonical(
            $versionKey,
            $generationKey,
            $prefix,
            'u',
            'result-query',
            'canonical-query',
        )[0]);
    }

    public function test_expired_owner_cannot_publish_or_release_a_replacement_lease(): void
    {
        $store = app(RedisStore::class);
        $key = 'test:{nc:x:lease}:result:u';
        $build = 'test:{nc:x:lease}:build';
        $wake = 'test:{nc:x:lease}:wake:' . str_repeat('a', 32);

        $store->setRaw($build, str_repeat('b', 32), 10);

        $this->assertFalse($store->publishVersionedEntries(
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

    public function test_repair_publication_is_version_and_lease_protected(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $repair = $keys->repairBuild($table, 'batch');
        $token = str_repeat('c', 32);
        $wake = $keys->repairWake($table, 'batch', $token);
        $row = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->setNxEx($repair, $token, 5));
        $this->assertTrue($store->publishVersionedEntries(
            entries: [$row => 'repaired'],
            ttl: 3600,
            versionKeys: [$keys->version($table), $keys->generation($table)],
            expectedVersions: ['0', '0'],
            buildingKey: $repair,
            wakeKey: $wake,
            token: $token,
            wakeTtl: 11,
        ));
        $this->assertSame('repaired', $store->getRaw($row));
        $this->assertNull($store->getRaw($repair));

        $mismatchRepair = $keys->repairBuild($table, 'version-mismatch');
        $mismatchToken = str_repeat('e', 32);
        $mismatchWake = $keys->repairWake($table, 'version-mismatch', $mismatchToken);
        $mismatchRow = $keys->row($table, '0', 'i:2');
        $store->increment($keys->version($table));

        $this->assertTrue($store->setNxEx($mismatchRepair, $mismatchToken, 5));
        $this->assertFalse($store->publishVersionedEntries(
            entries: [$mismatchRow => 'stale'],
            ttl: 3600,
            versionKeys: [$keys->version($table), $keys->generation($table)],
            expectedVersions: ['0', '0'],
            buildingKey: $mismatchRepair,
            wakeKey: $mismatchWake,
            token: $mismatchToken,
            wakeTtl: 11,
        ));
        $this->assertNull($store->getRaw($mismatchRow));
        $this->assertNull($store->getRaw($mismatchRepair));
    }
}
