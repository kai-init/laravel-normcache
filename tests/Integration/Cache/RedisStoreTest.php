<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\QueryEntryRepository;
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

    public int $evalShaCalls = 0;

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

        if (strtolower((string) $method) === 'evalsha') {
            $this->evalShaCalls++;
        }

        return null;
    }

    public function pipeline(...$arguments)
    {
        $this->pipelineCalls++;

        return [];
    }
}

final class RedisStoreTest extends TestCase
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

    public function test_cluster_table_invalidations_remain_separate_scripts(): void
    {
        $connection = new RecordingPredisClusterConnection(new Client);
        $store = new RedisStore('unused');
        $property = new ReflectionProperty($store, 'connection');
        $property->setValue($store, $connection);

        $store->invalidateTableStates([
            [
                'versionKey' => '{table-a}:version',
                'generationKey' => '{table-a}:generation',
                'mode' => 'generation',
                'tokens' => [],
                'rowPrefix' => '{table-a}:rows:',
                'changePrefix' => '{table-a}:chg:',
                'changePayload' => '',
                'changeTtl' => 60,
            ],
            [
                'versionKey' => '{table-b}:version',
                'generationKey' => '{table-b}:generation',
                'mode' => 'generation',
                'tokens' => [],
                'rowPrefix' => '{table-b}:rows:',
                'changePrefix' => '{table-b}:chg:',
                'changePayload' => '',
                'changeTtl' => 60,
            ],
        ]);

        $this->assertSame(2, $connection->evalShaCalls);
    }

    public function test_corrupt_result_read_does_not_delete_the_observed_payload(): void
    {
        $store = app(RedisStore::class);
        $key = 'test:{nc:x:corrupt-read}:payload';
        $store->setRawForever($key, 'corrupt');
        $state = new CacheState(
            key: $key,
            epoch: '0',
            version: '0',
            generation: '0',
            versions: [],
            tag: null,
            tagKey: null,
        );

        $result = app(QueryEntryRepository::class)->readResult($state, 'corrupt');

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
            $store->setRawForever($rawKey, 'raw-value');

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

    public function test_phpredis_lua_owner_comparison_supports_shared_serializer(): void
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

            $this->assertTrue($store->claimBuild($buildingKey, $token, 60)[0]);
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

    public function test_phpredis_unified_query_hash_protocol_supports_shared_serializer(): void
    {
        $connection = Redis::connection('normcache-test');

        if (!$connection instanceof PhpRedisConnection) {
            $this->markTestSkipped('PhpRedis only.');
        }

        $client = $connection->client();
        $originalSerializer = $client->getOption(\Redis::OPT_SERIALIZER);
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $this->assertNotNull($table);

        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $entryKey = $keys->queryEntry($table, 'u', 'serializer-query');
        $buildKey = $keys->queryBuild($table, '0', 'u', 'serializer-query');
        $token = str_repeat('a', 32);
        $wakeKey = $keys->wake($table, 'q', 'serializer-query', $token);

        try {
            $client->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_PHP);

            $this->assertTrue($store->claimBuild($buildKey, $token, 30)[0]);
            $this->assertTrue($store->publishCanonical(
                versionKey: $versionKey,
                generationKey: $generationKey,
                membershipKey: $entryKey,
                rowKeys: [],
                rowPayloads: [],
                expectedVersion: '0',
                expectedGeneration: '0',
                membershipPayload: 'membership-payload',
                membershipTtl: 60,
                rowTtl: 60,
                buildingKey: $buildKey,
                wakeKey: $wakeKey,
                token: $token,
                wakeTtl: 10,
                resultPayload: 'result-payload',
            ));

            $this->assertSame(
                ['result', '0', 'result-payload'],
                $store->fetchResultOrCanonical(
                    $versionKey,
                    $generationKey,
                    $keys->tablePrefix($table),
                    'u',
                    'serializer-query',
                    'serializer-query',
                ),
            );
            $this->assertSame('membership-payload', $store->readHashField($entryKey, 'm'));
            $this->assertSame('result-payload', $store->readHashField($entryKey, 'r'));
            $this->assertSame(
                \Redis::SERIALIZER_PHP,
                $client->getOption(\Redis::OPT_SERIALIZER),
            );
        } finally {
            $client->setOption(\Redis::OPT_SERIALIZER, \Redis::SERIALIZER_NONE);
            $connection->del($entryKey, $buildKey, $wakeKey);
            $client->setOption(\Redis::OPT_SERIALIZER, $originalSerializer);
        }
    }

    public function test_release_building_wakes_every_waiter_token(): void
    {
        $connection = Redis::connection('normcache-test');
        $store = new RedisStore('normcache-test');
        $buildingKey = 'test:{nc:x:wake-count}:build';
        $wakeKey = 'test:{nc:x:wake-count}:wake';
        $token = str_repeat('a', 32);

        try {
            $this->assertTrue($store->claimBuild($buildingKey, $token, 60)[0]);
            $this->assertTrue($store->releaseBuilding($buildingKey, $wakeKey, $token));
            $this->assertSame(64, $connection->llen($wakeKey));
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

            $store->setRawForever($key, 'replacement-raw-value');

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
        $membershipKey = $keys->queryEntry($table, 'u', 'query');
        $buildKey = $keys->queryBuild($table, '0', 'u', 'query');
        $token = str_repeat('a', 32);
        $wakeKey = $keys->wake($table, 'q', 'query', $token);
        $rowKey = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->claimBuild($buildKey, $token, 5)[0]);
        $this->assertTrue($store->publishCanonical(
            versionKey: $versionKey,
            generationKey: $generationKey,
            membershipKey: $membershipKey,
            rowKeys: [$rowKey],
            rowPayloads: ['row-payload'],
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

        $missed = $store->fetchCanonical(
            versionKey: $versionKey,
            generationKey: $generationKey,
            tablePrefix: $keys->tablePrefix($table),
            namespace: 'u',
            queryHash: 'never-written',
        );

        $this->assertSame('miss', $missed[0]);
        $this->assertSame('0', $missed[1]);
        $this->assertSame('0', $missed[2]);

        $store->increment($versionKey);
        $bumped = $store->fetchCanonical(
            $versionKey,
            $generationKey,
            $keys->tablePrefix($table),
            'u',
            'query',
        );
        $this->assertSame('hit', $bumped[0]);
        $this->assertSame('1', $bumped[1]);
    }

    public function test_canonical_publication_carries_the_result_overlay_under_the_same_guard(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->queryEntry($table, 'u', 'query');
        $buildKey = $keys->queryBuild($table, '0', 'u', 'query');
        $token = str_repeat('e', 32);
        $wakeKey = $keys->wake($table, 'q', 'query', $token);
        $rowKey = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->claimBuild($buildKey, $token, 5)[0]);
        $this->assertTrue($store->publishCanonical(
            versionKey: $versionKey,
            generationKey: $generationKey,
            membershipKey: $membershipKey,
            rowKeys: [$rowKey],
            rowPayloads: ['row-payload'],
            expectedVersion: '0',
            expectedGeneration: '0',
            membershipPayload: '{"f":4,"ep":"0","g":"0","ids":["i:1"],"vec":[]}',
            membershipTtl: 60,
            rowTtl: 3600,
            buildingKey: $buildKey,
            wakeKey: $wakeKey,
            token: $token,
            wakeTtl: 11,
            resultPayload: 'overlay-payload',
        ));

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
        $this->assertSame('overlay-payload', $store->readHashField($membershipKey, 'r'));
        $this->assertSame(60, Redis::connection('normcache-test')->ttl($membershipKey));
    }

    public function test_canonical_publication_rejects_the_result_overlay_after_state_changes(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->queryEntry($table, 'u', 'guarded');
        $buildKey = $keys->queryBuild($table, '0', 'u', 'guarded');
        $token = str_repeat('f', 32);
        $wakeKey = $keys->wake($table, 'q', 'guarded', $token);
        $rowKey = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->claimBuild($buildKey, $token, 5)[0]);
        $store->increment($generationKey);

        $this->assertFalse($store->publishCanonical(
            versionKey: $versionKey,
            generationKey: $generationKey,
            membershipKey: $membershipKey,
            rowKeys: [$rowKey],
            rowPayloads: ['row-payload'],
            expectedVersion: '0',
            expectedGeneration: '0',
            membershipPayload: '{"f":4,"ep":"0","g":"0","ids":["i:1"],"vec":[]}',
            membershipTtl: 60,
            rowTtl: 3600,
            buildingKey: $buildKey,
            wakeKey: $wakeKey,
            token: $token,
            wakeTtl: 11,
            resultPayload: 'overlay-payload',
        ));

        $this->assertNull($store->readHashField($membershipKey, 'm'));
        $this->assertNull($store->getRaw($rowKey));
        $this->assertNull($store->readHashField($membershipKey, 'r'));
    }

    public function test_canonical_publication_rejects_membership_after_state_changes(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $membershipKey = $keys->queryEntry($table, 'u', 'changed');
        $buildKey = $keys->queryBuild($table, '0', 'u', 'changed');
        $token = str_repeat('d', 32);
        $wakeKey = $keys->wake($table, 'q', 'changed', $token);
        $rowKey = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->claimBuild($buildKey, $token, 5)[0]);
        $store->increment($versionKey);

        $this->assertFalse($store->publishCanonical(
            $versionKey,
            $generationKey,
            $membershipKey,
            [$rowKey],
            ['row-payload'],
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
        $this->assertNull($store->readHashField($membershipKey, 'm'));
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
        $resultKey = $keys->queryEntry($table, 'u', 'result-query');
        $membershipKey = $keys->queryEntry($table, 'u', 'canonical-query');
        $membership = '{"f":4,"ep":"0","g":"0","ids":["i:1"],"vec":[]}';

        $store->writeHashField($membershipKey, 'm', $membership);
        $store->writeHashField($resultKey, 'r', 'result-payload');

        $result = $store->fetchResultOrCanonical(
            $versionKey,
            $generationKey,
            $prefix,
            'u',
            'result-query',
            'canonical-query',
        );

        $this->assertSame(['result', '0', 'result-payload'], $result);

        $store->deleteHashField($resultKey, 'r');
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
        $bumped = $store->fetchResultOrCanonical(
            $versionKey,
            $generationKey,
            $prefix,
            'u',
            'result-query',
            'canonical-query',
        );
        $this->assertSame('membership', $bumped[0]);
        $this->assertSame('1', $bumped[1]);
        $this->assertSame($membership, $bumped[3]);
    }

    public function test_multiple_table_states_are_invalidated_together(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $tables = app(TableIdentityResolver::class);
        $connection = $this->app['db']->connection();
        $posts = $tables->resolve($connection, 'posts');
        $authors = $tables->resolve($connection, 'authors');
        $authorRow = $keys->row($authors, '0', 'i:123');

        $store->setRawForever($authorRow, 'cached-row');
        $store->invalidateTableStates([
            [
                'versionKey' => $keys->version($posts),
                'generationKey' => $keys->generation($posts),
                'mode' => 'generation',
                'tokens' => ['i:123'],
                'rowPrefix' => $keys->tablePrefix($posts) . ':r:g',
                'changePrefix' => $keys->changeRecordPrefix($posts),
                'changePayload' => '',
                'changeTtl' => 60,
            ],
            [
                'versionKey' => $keys->version($authors),
                'generationKey' => $keys->generation($authors),
                'mode' => 'precise',
                'tokens' => ['i:123'],
                'rowPrefix' => $keys->tablePrefix($authors) . ':r:g',
                'changePrefix' => $keys->changeRecordPrefix($authors),
                'changePayload' => '',
                'changeTtl' => 60,
            ],
        ]);

        $this->assertSame('1', $store->getRaw($keys->version($posts)));
        $this->assertSame('1', $store->getRaw($keys->generation($posts)));
        $this->assertSame('1', $store->getRaw($keys->version($authors)));
        $this->assertNull($store->getRaw($authorRow));
    }

    public function test_expired_owner_cannot_publish_or_release_a_replacement_lease(): void
    {
        $store = app(RedisStore::class);
        $key = 'test:{nc:x:lease}:result:u';
        $build = 'test:{nc:x:lease}:build';
        $wake = 'test:{nc:x:lease}:wake:' . str_repeat('a', 32);

        $store->setRawForever($build, str_repeat('b', 32));

        $this->assertFalse($store->publishVersionedEntries(
            [$key],
            ['stale'],
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

    public function test_repair_publication_is_version_protected_without_a_lease(): void
    {
        $store = app(RedisStore::class);
        $keys = app(CacheKeyBuilder::class);
        $table = app(TableIdentityResolver::class)
            ->resolve($this->app['db']->connection(), 'posts');
        $row = $keys->row($table, '0', 'i:1');

        $this->assertTrue($store->publishVersionedEntries(
            entryKeys: [$row],
            entryPayloads: ['repaired'],
            ttl: 3600,
            versionKeys: [$keys->version($table), $keys->generation($table)],
            expectedVersions: ['0', '0'],
        ));
        $this->assertSame('repaired', $store->getRaw($row));

        $mismatchRow = $keys->row($table, '0', 'i:2');
        $store->increment($keys->version($table));

        $this->assertFalse($store->publishVersionedEntries(
            entryKeys: [$mismatchRow],
            entryPayloads: ['stale'],
            ttl: 3600,
            versionKeys: [$keys->version($table), $keys->generation($table)],
            expectedVersions: ['0', '0'],
        ));
        $this->assertNull($store->getRaw($mismatchRow));
    }
}
