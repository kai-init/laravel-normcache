<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\RedisScripts;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableQueryReply;
use PHPUnit\Framework\Attributes\DataProvider;

final class CanonicalBuildLeaseTest extends TestCase
{
    /** @return array<string, array{int}> */
    public static function overlayModes(): array
    {
        return ['overlay enabled' => [1000], 'overlay disabled' => [0]];
    }

    #[DataProvider('overlayModes')]
    public function test_clean_canonical_miss_claims_once_then_warms_direct_row_reads(int $overlayRows): void
    {
        $values = config('normcache');
        $values['auto_overlay_max_rows'] = $overlayRows;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($values));

        $author = Author::query()->create(['name' => 'Lease author']);
        $id = RawPost::query()->toBase()->insertGetId([
            'title' => 'Lease post', 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);
        $query = RawPost::query()->toBase()->where('title', 'Lease post');
        DB::flushQueryLog();
        DB::enableQueryLog();
        $query->get();
        $coldQueries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertCount(2, $coldQueries);

        $this->assertSame([], $this->cacheKeysMatching(':build:q:'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $query->get();
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());
        $this->assertSame([], $this->cacheKeysMatching(':build:q:'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = RawPost::query()->toBase()->where('id', $id)->first();
        DB::disableQueryLog();

        $this->assertSame('Lease post', $row?->title);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_corrupt_membership_uses_the_existing_rebuild_fallback(): void
    {
        $author = Author::query()->create(['name' => 'Corrupt lease author']);
        RawPost::query()->toBase()->insert([
            'title' => 'Corrupt lease post', 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);
        $query = fn() => RawPost::query()->toBase()->where('title', 'Corrupt lease post')->get();
        $query();
        $entry = $this->cacheQueryKeysWithField('m')[0];
        $this->deleteCacheField($entry, 'r');
        $this->writeCacheField($entry, 'm', 'not-a-membership');

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $query();
        $sql = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame(['Corrupt lease post'], $actual->pluck('title')->all());
        $this->assertNotSame([], $sql);
        $this->assertNotSame('not-a-membership', $this->cacheStore()->readHashField($entry, 'm'));
        $this->assertSame([], $this->cacheKeysMatching(':build:q:'));
    }

    #[DataProvider('fusedMissModes')]
    public function test_fused_miss_claim_protocol_reports_owner_waiter_and_retry(string $mode): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($table);
        $keys = $this->cacheKeys();
        $version = $keys->version($table);
        $generation = $keys->generation($table);
        $prefix = $keys->tablePrefix($table);
        $hash = 'lease-protocol';
        $build = $keys->queryBuild($table, '0', 'n', $hash);
        $token = str_repeat('a', 32);

        $plain = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, $mode);
        $this->assertSame('miss', $plain[0]);
        $this->assertSame('0', $plain[1]);
        $this->assertSame('0', $plain[2]);
        $this->assertNull($this->cacheStore()->getRaw($build));

        $owner = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, $mode, $token, 30);
        $this->assertSame('miss', $owner[0]);
        $this->assertSame('0', $owner[1]);
        $this->assertSame('0', $owner[2]);
        $this->assertSame(1, (int) $owner[4]);
        $this->assertSame($token, $owner[5]);

        $waiter = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, $mode, str_repeat('b', 32), 30);
        $this->assertSame('miss', $waiter[0]);
        $this->assertSame('0', $waiter[1]);
        $this->assertSame('0', $waiter[2]);
        $this->assertSame(0, (int) $waiter[4]);
        $this->assertSame($token, $waiter[5]);
        $this->assertSame($token, $this->cacheStore()->getRaw($build));

        $retry = $this->cacheStore()->fetchTableQuery($version, $generation, $prefix, 'n', $hash, $mode, $token, 30);
        $this->assertSame('miss', $retry[0]);
        $this->assertSame('0', $retry[1]);
        $this->assertSame('0', $retry[2]);
        $this->assertSame(1, (int) $retry[4]);
        $this->assertSame($token, $retry[5]);
    }

    /** @return array<string, array{string}> */
    public static function fusedMissModes(): array
    {
        return [
            'membership' => ['membership'],
            'prefer-result' => ['prefer-result'],
        ];
    }

    public function test_engine_retry_after_owner_release_does_not_claim_again(): void
    {
        $author = Author::query()->create(['name' => 'Waiter owner']);
        $id = RawPost::query()->toBase()->insertGetId([
            'title' => 'Waiter post', 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);
        $query = fn() => RawPost::query()->toBase()->where('title', 'Waiter post')->get();
        $query();
        $entry = $this->cacheQueryKeysWithField('m')[0];
        preg_match('/:q:v([^:]+):u:(.+)$/', $entry, $match);
        $this->assertCount(3, $match);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);
        $keys = $this->cacheKeys();
        $hash = $match[2];
        $version = $match[1];
        $buildKey = $keys->queryBuild($table, $version, 'u', $hash);
        $ownerToken = str_repeat('c', 32);
        $this->claimBuildForTest($buildKey, $ownerToken, 20);
        $this->deleteCacheKeys($entry);
        $queryBuilder = RawPost::query()->toBase()->where('title', 'Waiter post');
        $plan = app(QueryPlanner::class)->plan($queryBuilder, $table, [$table]);
        $cached = app(QueryEntryRepository::class)->read($queryBuilder, $plan, 'u', $hash, claim: true);
        $this->assertNotNull($cached->lease);
        $this->assertFalse($cached->lease->owner);
        $this->assertSame($ownerToken, $cached->lease->token);
        $this->assertSame($keys->wake($table, 'q', $hash, $ownerToken), $cached->lease->wakeKey);
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $released = false;
        $connection->listen(function (CommandExecuted $event) use (&$released, $buildKey, $ownerToken, $keys, $table, $hash): void {
            if (!$released && strtolower($event->command) === 'brpop') {
                $released = true;
                $this->cacheStore()->releaseBuilding(
                    $buildKey,
                    $keys->wake($table, 'q', $hash, $ownerToken),
                    $ownerToken,
                    10,
                );
            }
        });

        try {
            $result = $query();
        } finally {
            $connection->unsetEventDispatcher();
        }

        $this->assertTrue($released);
        $this->assertSame(['Waiter post'], $result->pluck('title')->all());
        $this->assertNull($this->cacheStore()->getRaw($buildKey));
    }

    public function test_resolution_exception_releases_and_wakes_the_fused_owner(): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);
        $keys = $this->cacheKeys();
        $buildKey = $keys->queryBuild($table, '0', 'u', 'throwing-resolution');
        $owner = str_repeat('d', 32);
        $this->claimBuildForTest($buildKey, $owner, 30);
        $dependency = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($dependency);
        $plan = QueryPlan::canonical($table, [$table, $dependency], new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER));
        $repository = $this->app->make(QueryEntryRepository::class);
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $thrown = false;
        $connection->listen(function (CommandExecuted $event) use (&$thrown): void {
            if (!$thrown && in_array(strtolower($event->command), ['mget', 'pipeline'], true)) {
                $thrown = true;
                throw new \Error('resolution failed');
            }
        });

        try {
            $repository->readCanonical(
                RawPost::query()->toBase(),
                $plan,
                'u',
                'throwing-resolution',
                new TableQueryReply('miss', '0', '0', null, true, $owner),
            );
            $this->fail('Expected state resolution to throw.');
        } catch (\Error $exception) {
            $this->assertSame('resolution failed', $exception->getMessage());
        } finally {
            $connection->unsetEventDispatcher();
        }

        $this->assertTrue($thrown);
        $this->assertNull($this->cacheStore()->getRaw($buildKey));
        $this->assertTrue($this->cacheStore()->brpop($keys->wake($table, 'q', 'throwing-resolution', $owner), 0.1));
    }

    public function test_canonical_pre_lua_rejection_releases_and_wakes_the_owner(): void
    {
        $query = Author::query()->toBase()->where('name', 'canonical-pre-lua')->limit(1);
        $table = app(TableIdentityResolver::class)->resolve($query->getConnection(), 'authors');
        $this->assertNotNull($table);
        $plan = app(QueryPlanner::class)->plan($query, $table, [$table]);
        $this->assertTrue($plan->isCanonical());
        $hash = 'canonical-pre-lua';
        $repository = app(QueryEntryRepository::class);
        $read = $repository->read($query, $plan, 'u', $hash, claim: true);
        $this->assertNotNull($read->lease);
        $this->assertTrue($read->lease->owner);

        $row = new \stdClass;
        $row->id = 987654;
        $row->name = 'canonical-pre-lua';
        $stream = fopen('php://memory', 'r+');

        try {
            $row->resource = $stream;
            $published = $repository->publishCanonical(
                $query,
                $plan,
                $read->state,
                [$row],
                $read->lease,
                app(CacheConfig::class)->wakeTtl(),
            );
        } finally {
            fclose($stream);
        }

        $this->assertFalse($published);
        $this->assertNull($this->cacheStore()->readHashField($read->state->key, 'm'));
        $this->assertNull($this->cacheStore()->readHashField($read->state->key, 'r'));
        $this->assertNull($this->cacheStore()->getRaw($read->lease->buildingKey));
        $this->assertSame(
            app(CacheConfig::class)->stampedeWakeTokens,
            Redis::connection('normcache-test')->llen($read->lease->wakeKey),
        );
    }

    public function test_canonical_lua_version_guard_rejection_releases_and_wakes_the_owner(): void
    {
        $name = 'canonical-version-guard-' . bin2hex(random_bytes(8));
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
            $canonicalSha = sha1(RedisScripts::get('publish_canonical'));
            $redisConnection->listen(function (CommandExecuted $event) use (
                &$releaseOnlyCalls,
                &$normalPublicationCalls,
                $scriptSha,
                $canonicalSha,
            ): void {
                $command = strtolower($event->command);
                $parameters = $event->parameters;
                $matchesCanonical = $command === 'evalsha'
                    ? strtolower((string) ($parameters[0] ?? '')) === $canonicalSha
                    : ($command === 'eval' && ($parameters[0] ?? null) === RedisScripts::get('publish_canonical'));

                if ($matchesCanonical) {
                    $normalPublicationCalls++;

                    return;
                }

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
            $rows = Author::query()->where('name', $name)->limit(1)->get();
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
