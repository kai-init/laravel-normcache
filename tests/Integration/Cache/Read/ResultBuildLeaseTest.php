<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
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
        $prefix = $keys->tablePrefix($table);
        $build = $keys->queryBuild($table, '0', 'n', $hash);

        $this->assertSame(['0'], $this->cacheStore()->fetchResult($version, $prefix, 'n', $hash));
        $this->assertNull($this->cacheStore()->getRaw($build));
        $ownerToken = str_repeat('a', 32);
        $differentToken = str_repeat('b', 32);
        foreach ([[$ownerToken, 1], [$ownerToken, 1], [$differentToken, 0]] as [$token, $expectedOwner]) {
            $reply = $this->cacheStore()->fetchResult($version, $prefix, 'n', $hash, $token, 30);
            $this->assertCount(4, $reply);
            // PhpRedis returns false for an absent bulk value; Predis returns null.
            $reply[1] ??= false;
            $this->assertSame(['0', false, $expectedOwner, $ownerToken], $reply);
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
        $this->assertSame(['0', 'payload'], $this->cacheStore()->fetchResult($version, $prefix, 'n', $hash, str_repeat('c', 32), 30));
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
        $waiter = app(QueryEntryRepository::class)->read($builder, $plan, 'u', $hash, claim: true);
        $this->assertNotNull($waiter->lease);
        $this->assertFalse($waiter->lease->owner);
        $this->assertSame($ownerToken, $waiter->lease->token);
        $this->assertSame($keys->wake($table, 'q', $hash, $ownerToken), $waiter->lease->wakeKey);

        $retry = app(QueryEntryRepository::class)->read($builder, $plan, 'u', $hash, claim: false);
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
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $fired = false;
        $ownerToken = null;
        $connection->listen(function (CommandExecuted $event) use (&$fired, &$ownerToken, $buildKey): void {
            if (!$fired && strtolower($event->command) === 'mget') {
                $fired = true;
                $ownerToken = $this->cacheStore()->getRaw($buildKey);
                throw new \Error('result resolution failed');
            }
        });

        try {
            $repository->read($builder, $plan, 'u', $hash, claim: true);
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
}
