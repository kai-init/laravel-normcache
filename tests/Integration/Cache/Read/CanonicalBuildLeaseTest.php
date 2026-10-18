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
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
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

    #[DataProvider('fusedMissFetchers')]
    public function test_fused_miss_claim_protocol_reports_owner_waiter_and_retry(string $fetcher): void
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

        $plain = $this->cacheStore()->{$fetcher}($version, $generation, $prefix, 'n', $hash);
        $this->assertSame(['miss', '0', '0'], $plain);
        $this->assertNull($this->cacheStore()->getRaw($build));

        $owner = $this->cacheStore()->{$fetcher}($version, $generation, $prefix, 'n', $hash, $token, 30);
        $this->assertSame(['miss', '0', '0', 1, $token], $owner);

        $waiter = $this->cacheStore()->{$fetcher}($version, $generation, $prefix, 'n', $hash, str_repeat('b', 32), 30);
        $this->assertSame(['miss', '0', '0', 0, $token], $waiter);
        $this->assertSame($token, $this->cacheStore()->getRaw($build));

        $retry = $this->cacheStore()->{$fetcher}($version, $generation, $prefix, 'n', $hash, $token, 30);
        $this->assertSame(['miss', '0', '0', 1, $token], $retry);
    }

    /** @return array<string, array{string}> */
    public static function fusedMissFetchers(): array
    {
        return [
            'canonical' => ['fetchCanonical'],
            'result or canonical' => ['fetchResultOrCanonical'],
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
                ['miss', '0', '0', 1, $owner],
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
}
