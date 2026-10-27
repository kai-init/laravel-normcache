<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Cache\QueryHashResolver;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\RedisScripts;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\QueryPlan;

final class ResultBuildLeaseTest extends TestCase
{
    public function test_result_read_only_and_hit_shapes_stay_unchanged_while_claims_are_retry_safe(): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($table);
        $keys = $this->cacheKeys();
        $hash = 'result-lease-protocol';
        $version = $keys->version($table);
        $generation = $keys->generation($table);
        $prefix = $keys->tablePrefix($table);
        $build = $keys->queryBuild($table, '0', 'n', $hash);

        $unclaimed = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, 'result');
        $this->assertSame('miss', $unclaimed[0]);
        $this->assertSame('0', $unclaimed[1]);
        $this->assertNull($this->cacheStore()->getRaw($build));
        $ownerToken = str_repeat('a', 32);
        $differentToken = str_repeat('b', 32);
        foreach ([[$ownerToken, 1], [$ownerToken, 1], [$differentToken, 0]] as [$token, $expectedOwner]) {
            $reply = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, 'result', $token, 30);
            $this->assertCount(6, $reply);
            $this->assertSame('miss', $reply[0]);
            $this->assertSame('0', $reply[1]);
            $this->assertSame($expectedOwner, (int) $reply[4]);
            $this->assertSame($ownerToken, $reply[5]);
        }

        $this->assertSame($ownerToken, $this->cacheStore()->getRaw($build));
        $this->cacheStore()->releaseBuilding(
            $build,
            $keys->wake($table, 'q', $hash, $ownerToken),
            $ownerToken,
            10,
        );

        $entry = $keys->queryEntry($table, '0', 'n', $hash);
        $this->writeCacheField($entry, 'r', 'payload');
        $hit = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, 'result', str_repeat('c', 32), 30);
        $this->assertSame('result', $hit[0]);
        $this->assertSame('0', $hit[1]);
        $this->assertSame('payload', $hit[3]);
        $this->assertNull($this->cacheStore()->getRaw($build));
    }

    public function test_projected_cold_result_warms_without_stranding_query_build_lease(): void
    {
        $author = Author::query()->create(['name' => 'Result lease author']);
        RawPost::query()->toBase()->insert([
            'title' => 'Projected lease', 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);
        $query = fn() => RawPost::query()->toBase()
            ->where('title', 'Projected lease')
            ->select('title as heading')
            ->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $cold = $query();
        $coldSql = DB::getQueryLog();
        DB::flushQueryLog();
        $warm = $query();
        $warmSql = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame([['heading' => 'Projected lease']], $cold->map(static fn(object $row): array => (array) $row)->all());
        $this->assertSame($cold->map(static fn(object $row): array => (array) $row)->all(), $warm->map(static fn(object $row): array => (array) $row)->all());
        $this->assertCount(1, $coldSql);
        $this->assertSame([], $warmSql);
        $this->assertSame([], $this->cacheKeysMatching(':build:q:'));
    }

    public function test_projected_result_waiter_uses_existing_owner_and_retry_does_not_claim(): void
    {
        $author = Author::query()->create(['name' => 'Result lease waiter']);
        RawPost::query()->toBase()->insert([
            'title' => 'Projected waiter', 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);
        $builder = RawPost::query()->toBase()
            ->where('title', 'Projected waiter')
            ->select('title as heading');
        $builder->get();

        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);
        $entry = $this->cacheQueryKeysWithField('r')[0];
        $prefix = preg_quote($this->cacheKeys()->tablePrefix($table), '/');
        $this->assertSame(1, preg_match('/^' . $prefix . ':q:v([^:]+):u:(.+)$/', $entry, $match));
        $version = $match[1];
        $hash = $match[2];
        $keys = $this->cacheKeys();
        $buildKey = $keys->queryBuild($table, $version, 'u', $hash);
        $ownerToken = str_repeat('e', 32);
        $this->assertTrue($this->claimBuildForTest($buildKey, $ownerToken, 30));
        $this->deleteCacheField($entry, 'r');

        $plan = app(QueryPlanner::class)->plan($builder, $table, [$table]);
        $waiter = app(QueryEntryRepository::class)->read($builder, $plan, 'u', new QueryHashResolver(static fn(): string => $hash), claim: true);
        $this->assertNotNull($waiter->lease);
        $this->assertFalse($waiter->lease->owner);
        $this->assertSame($ownerToken, $waiter->lease->token);
        $this->assertSame($keys->wake($table, 'q', $hash, $ownerToken), $waiter->lease->wakeKey);

        $retry = app(QueryEntryRepository::class)->read($builder, $plan, 'u', new QueryHashResolver(static fn(): string => $hash), claim: false);
        $this->assertFalse($retry->served());
        $this->assertNull($retry->lease);
        $this->assertSame($ownerToken, $this->cacheStore()->getRaw($buildKey));
        $this->cacheStore()->releaseBuilding($buildKey, $keys->wake($table, 'q', $hash, $ownerToken), $ownerToken, 10);
    }

    public function test_result_resolution_exception_releases_and_wakes_the_fused_owner(): void
    {
        $tableResolver = app(TableIdentityResolver::class);
        $posts = $tableResolver->resolve(DB::connection(), 'posts');
        $authors = $tableResolver->resolve(DB::connection(), 'authors');
        $this->assertNotNull($posts);
        $this->assertNotNull($authors);
        $plan = QueryPlan::result($posts, [$posts, $authors]);
        $this->assertCount(2, $plan->dependencies);
        $builder = RawPost::query()->toBase()->select('posts.title');
        $hash = 'result-resolution-exception';
        $version = $this->cacheStore()->getRaw($this->cacheKeys()->version($posts)) ?? '0';
        $buildKey = $this->cacheKeys()->queryBuild($posts, $version, 'u', $hash);
        $repository = $this->app->make(QueryEntryRepository::class);
        $this->app->make(CacheRuntime::class)->epoch();
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $fired = false;
        $ownerToken = null;
        $connection->listen(function (CommandExecuted $event) use (&$fired, &$ownerToken, $buildKey): void {
            if (!$fired && in_array(strtolower($event->command), ['mget', 'pipeline'], true)) {
                $fired = true;
                $ownerToken = $this->cacheStore()->getRaw($buildKey);
                throw new \Error('result resolution failed');
            }
        });

        try {
            $repository->read($builder, $plan, 'u', new QueryHashResolver(static fn(): string => $hash), claim: true);
            $this->fail('Expected result state resolution to throw.');
        } catch (\Error $exception) {
            $this->assertSame('result resolution failed', $exception->getMessage());
        } finally {
            $connection->unsetEventDispatcher();
        }

        $this->assertTrue($fired);
        $this->assertIsString($ownerToken);
        $this->assertSame(32, strlen($ownerToken));
        $this->assertNull($this->cacheStore()->getRaw($buildKey));
        $this->assertTrue($this->cacheStore()->brpop(
            $this->cacheKeys()->wake($posts, 'q', $hash, $ownerToken),
            0.1,
        ));
    }

    public function test_result_pre_lua_rejection_releases_and_wakes_the_owner(): void
    {
        $query = Author::query()->toBase()->select('name')->where('name', 'result-pre-lua')->limit(1);
        $table = app(TableIdentityResolver::class)->resolve($query->getConnection(), 'authors');
        $this->assertNotNull($table);
        $plan = app(QueryPlanner::class)->plan($query, $table, [$table]);
        $this->assertTrue($plan->isResult());
        $hash = 'result-pre-lua';
        $repository = app(QueryEntryRepository::class);
        $read = $repository->read($query, $plan, 'u', new QueryHashResolver(static fn(): string => $hash), claim: true);
        $this->assertNotNull($read->lease);
        $this->assertTrue($read->lease->owner);

        $row = new \stdClass;
        $row->name = 'result-pre-lua';
        $stream = fopen('php://memory', 'r+');

        try {
            $row->resource = $stream;
            $published = $repository->publish(
                $query,
                $plan,
                $read->state,
                [$row],
                $read->lease,
            );
        } finally {
            fclose($stream);
        }

        $this->assertFalse($published);
        $this->assertNull($this->cacheStore()->readHashField($read->state->key, 'r'));
        $this->assertNull($this->cacheStore()->getRaw($read->lease->buildingKey));
        $this->assertSame(
            app(CacheConfig::class)->stampedeWakeTokens,
            Redis::connection('normcache-test')->llen($read->lease->wakeKey),
        );
    }

    public function test_result_lua_version_guard_rejection_releases_and_wakes_the_owner(): void
    {
        $name = 'result-version-guard-' . bin2hex(random_bytes(8));
        Author::query()->create(['name' => $name]);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($table);
        $keys = $this->cacheKeys();
        $buildKey = null;
        $wakeKey = null;
        $oldEntryKey = null;
        $advanced = false;
        $selectCount = 0;
        $releaseOnlyCalls = 0;
        $normalPublicationCalls = 0;
        $redisConnection = Redis::connection('normcache-test');
        $isPhpRedis = $redisConnection instanceof PhpRedisConnection;
        $redisConnection->setEventDispatcher($this->app->make('events'));

        if (!$isPhpRedis) {
            $scriptSha = sha1(RedisScripts::get('publish_versioned_entries'));
            $redisConnection->listen(function (CommandExecuted $event) use (
                &$releaseOnlyCalls,
                &$normalPublicationCalls,
                $scriptSha,
            ): void {
                $command = strtolower($event->command);
                $parameters = $event->parameters;
                $matchesScript = $command === 'evalsha'
                    ? strtolower((string) ($parameters[0] ?? '')) === $scriptSha
                    : ($command === 'eval' && ($parameters[0] ?? null) === RedisScripts::get('publish_versioned_entries'));

                if (!$matchesScript) {
                    return;
                }

                $argumentStart = 2 + (int) ($parameters[1] ?? 0);
                if (
                    (int) ($parameters[$argumentStart] ?? -1) === 0
                    && (int) ($parameters[$argumentStart + 1] ?? -1) === 0
                ) {
                    $releaseOnlyCalls++;
                } elseif (
                    (int) ($parameters[$argumentStart] ?? 0) > 0
                    && (int) ($parameters[$argumentStart + 1] ?? 0) > 0
                ) {
                    $normalPublicationCalls++;
                }
            });
        }

        DB::listen(function (QueryExecuted $event) use (
            &$advanced,
            &$selectCount,
            &$buildKey,
            &$wakeKey,
            &$oldEntryKey,
            $name,
            $table,
            $keys,
        ): void {
            $sql = strtolower(ltrim($event->sql));
            if (
                $advanced
                || !str_starts_with($sql, 'select')
                || !str_contains($sql, 'authors')
                || !str_contains($sql, 'limit 1')
                || !in_array($name, $event->bindings, true)
            ) {
                return;
            }

            $advanced = true;
            $selectCount++;
            $buildKeys = $this->cacheKeysMatching($keys->tablePrefix($table) . ':build:q:');
            $this->assertCount(1, $buildKeys);
            $buildKey = $buildKeys[0];
            $this->assertSame(1, preg_match('/\\:build:q:v([^:]+):([^:]+):([a-f0-9]{32})$/', $buildKey, $match));
            $token = $this->cacheStore()->getRaw($buildKey);
            $this->assertIsString($token);
            $oldEntryKey = $keys->queryEntry($table, $match[1], $match[2], $match[3]);
            $wakeKey = $keys->wake($table, 'q', $match[3], $token);
            $this->cacheStore()->increment($keys->version($table));
        });

        try {
            $rows = Author::query()->select('name')->where('name', $name)->limit(1)->get();
        } finally {
            $redisConnection->unsetEventDispatcher();
        }

        $this->assertTrue($advanced);
        $this->assertSame(1, $selectCount);
        $this->assertSame([$name], $rows->pluck('name')->all());
        $this->assertNotNull($buildKey);
        $this->assertNotNull($wakeKey);
        $this->assertNotNull($oldEntryKey);
        $this->assertNull($this->cacheStore()->readHashField($oldEntryKey, 'm'));
        $this->assertNull($this->cacheStore()->readHashField($oldEntryKey, 'r'));
        $this->assertNull($this->cacheStore()->getRaw($buildKey));
        $this->assertSame(
            app(CacheConfig::class)->stampedeWakeTokens,
            $redisConnection->llen($wakeKey),
        );

        if (!$isPhpRedis) {
            $this->assertSame(0, $releaseOnlyCalls);
            $this->assertSame(1, $normalPublicationCalls);
        }
    }
}
