<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Cache\QueryHashResolver;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use PHPUnit\Framework\Attributes\DataProvider;

final class ResultBuildLeaseTest extends TestCase
{
    public function test_full_result_reads_do_not_claim_a_lease_and_waiters_do_not_release_the_owner(): void
    {
        $query = Author::query()->toBase()->select('name');
        $table = app(TableIdentityResolver::class)->resolve($query->getConnection(), 'authors');
        $this->assertNotNull($table);
        $plan = app(QueryPlanner::class)->plan($query, $table, [$table]);
        $hash = str_repeat('a', 32);
        $keys = $this->cacheKeys();
        $repository = app(QueryEntryRepository::class);

        $read = $repository->read($query, $plan, 'u', new QueryHashResolver(static fn(): string => $hash), claim: true);
        $this->assertFalse($read->served());
        $this->assertNull($read->lease);
        $this->assertNull($this->cacheStore()->getRaw($keys->resultBuild($hash)));

        $leases = app(BuildLeaseCoordinator::class);
        $owner = $leases->claim($plan, $read->state, 'u', $hash);
        $waiter = $leases->claim($plan, $read->state, 'u', $hash);
        $this->assertTrue($owner->owner);
        $this->assertFalse($waiter->owner);
        $this->assertSame($owner->token, $waiter->token);
        $this->assertSame($keys->resultWake($hash, $owner->token), $waiter->wakeKey);

        $leases->release($waiter);
        $this->assertSame($owner->token, $this->cacheStore()->getRaw($owner->buildingKey));
        $leases->release($owner);
        $this->assertNull($this->cacheStore()->getRaw($owner->buildingKey));
        $this->assertTrue($this->cacheStore()->brpop($owner->wakeKey, 0.1));
    }

    public function test_projected_cold_result_warms_without_stranding_its_lease(): void
    {
        $author = Author::query()->create(['name' => 'Result lease author']);
        RawPost::query()->toBase()->insert([
            'title' => 'Projected lease', 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);
        $query = static fn() => RawPost::query()->toBase()
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

        $expected = [['heading' => 'Projected lease']];
        $this->assertSame($expected, $cold->map(static fn(object $row): array => (array) $row)->all());
        $this->assertSame($expected, $warm->map(static fn(object $row): array => (array) $row)->all());
        $this->assertCount(1, $coldSql);
        $this->assertSame([], $warmSql);

        $entries = $this->cacheQueryKeysWithField('r');
        $this->assertCount(1, $entries);
        $this->assertSame(1, preg_match('/\\{nc:x:([a-f0-9]{32})\\}:q:u$/', $entries[0], $matches));
        $this->assertNull($this->cacheStore()->getRaw($this->cacheKeys()->resultBuild($matches[1])));
    }

    public function test_uncacheable_result_settles_its_lease_before_redis_publication(): void
    {
        $query = Author::query()->toBase()->select('name');
        $table = app(TableIdentityResolver::class)->resolve($query->getConnection(), 'authors');
        $this->assertNotNull($table);
        $plan = app(QueryPlanner::class)->plan($query, $table, [$table]);
        $hash = 'result-pre-lua';
        $repository = app(QueryEntryRepository::class);
        $read = $repository->read($query, $plan, 'u', new QueryHashResolver(static fn(): string => $hash));
        $lease = app(BuildLeaseCoordinator::class)->claim($plan, $read->state, 'u', $hash);
        $this->assertTrue($lease->owner);

        $row = new \stdClass;
        $row->name = 'result-pre-lua';
        $stream = fopen('php://memory', 'r+');
        try {
            $row->resource = $stream;
            $published = $repository->publish($query, $plan, $read->state, [$row], $lease);
        } finally {
            fclose($stream);
        }

        $this->assertFalse($published);
        $this->assertNull($this->cacheStore()->readHashField($read->state->key, 'r'));
        $this->assertNull($this->cacheStore()->getRaw($lease->buildingKey));
        $this->assertSame(
            app(CacheConfig::class)->stampedeWakeTokens,
            Redis::connection('normcache-test')->llen($lease->wakeKey),
        );
    }

    public function test_late_stale_full_result_publication_cannot_be_served(): void
    {
        $author = Author::query()->create(['name' => 'Before']);
        $read = static fn() => Author::query()->select('name')->whereKey($author->getKey())->get();

        $this->assertSame(['Before'], $read()->pluck('name')->all());
        $entry = $this->cacheQueryKeysWithField('r')[0];
        $oldPayload = $this->cacheStore()->readHashField($entry, 'r');
        $this->assertIsString($oldPayload);

        Author::query()->whereKey($author->getKey())->update(['name' => 'After']);
        $this->writeCacheField($entry, 'r', $oldPayload);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame(['After'], $read()->pluck('name')->all());
        $this->assertCount(1, DB::getQueryLog());
        DB::flushQueryLog();
        $this->assertSame(['After'], $read()->pluck('name')->all());
        $this->assertSame([], DB::getQueryLog());
        DB::disableQueryLog();
    }

    #[DataProvider('invalidatedTables')]
    public function test_invalidation_during_full_result_sql_does_not_publish_stale_data(bool $joined): void
    {
        $name = 'full-result-race-' . ($joined ? 'dependency' : 'root');
        $author = Author::query()->create(['name' => $name]);
        RawPost::query()->toBase()->insert([
            'title' => $name, 'views' => 1, 'published' => true,
            'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
        ]);

        $query = $joined
            ? static fn() => RawPost::query()->toBase()
                ->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('authors.name')
                ->where('authors.name', $name)
                ->get()
            : static fn() => Author::query()
                ->select('name')
                ->where('name', $name)
                ->get();

        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($table);
        $advanced = false;
        DB::listen(function (QueryExecuted $event) use (&$advanced, $name, $table): void {
            if ($advanced || !in_array($name, $event->bindings, true)) {
                return;
            }

            $advanced = true;
            $this->cacheStore()->increment($this->cacheKeys()->version($table));
        });

        $this->assertSame([$name], $query()->pluck('name')->all());
        $this->assertTrue($advanced);
        $this->assertSame([], $this->cacheQueryKeysWithField('r'));

        $this->assertSame([$name], $query()->pluck('name')->all());
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame([$name], $query()->pluck('name')->all());
        $sql = DB::getQueryLog();
        DB::disableQueryLog();
        $this->assertSame([], $sql);
    }

    public static function invalidatedTables(): array
    {
        return [
            'root version' => [false],
            'joined dependency version' => [true],
        ];
    }
}
