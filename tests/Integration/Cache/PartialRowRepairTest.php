<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Payload\MembershipCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;
use PHPUnit\Framework\Attributes\DataProvider;

final class PartialRowRepairTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('normcache.auto_overlay_max_rows', 0);
    }

    public function test_only_missing_entities_are_fetched_and_membership_order_is_preserved(): void
    {
        foreach (['One', 'Two', 'Three', 'Four'] as $name) {
            Author::create(['name' => $name]);
        }

        $query = fn() => Author::orderByDesc('id')->offset(1)->limit(3)->get();
        $expected = $query()->toArray();
        $missingId = $expected[1]['id'];
        $rowKey = $this->authorRowKey($missingId);
        $this->cacheStore()->delete($rowKey);
        $membershipKey = $this->cacheQueryKeysWithField('m')[0];
        $membership = $this->cacheStore()->readHashField($membershipKey, 'm');
        Redis::connection('normcache-test')->expire($membershipKey, 30);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $query()->toArray();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame($expected, $actual);
        $this->assertCount(1, $queries);
        $this->assertSame([$missingId], $queries[0]['bindings']);
        $this->assertStringContainsString(' in (', $queries[0]['query']);
        $this->assertStringNotContainsString('offset', $queries[0]['query']);
        $this->assertNotNull($this->cacheStore()->getRaw($rowKey));
        $this->assertSame($membership, $this->cacheStore()->readHashField($membershipKey, 'm'));
        $this->assertLessThanOrEqual(30, Redis::connection('normcache-test')->ttl($membershipKey));
        $this->assertSame([], $this->cacheQueryKeysWithField('r'));
        $this->assertWarmCacheHit($query);
    }

    public function test_a_write_during_repair_discards_the_old_membership(): void
    {
        $first = Author::create(['name' => 'Included']);
        Author::create(['name' => 'Included']);
        $query = fn() => Author::where('name', 'Included')->orderBy('id')->get();
        $query();
        $this->cacheStore()->delete($this->authorRowKey($first->id));
        $mutated = false;
        DB::listen(function ($event) use (&$mutated, $first): void {
            if (!$mutated && str_contains($event->sql, ' in (')) {
                $mutated = true;
                $first->update(['name' => 'Excluded']);
            }
        });

        $actual = $query();

        $this->assertTrue($mutated);
        $this->assertSame(Author::withoutCache()->where('name', 'Included')->orderBy('id')->get()->modelKeys(), $actual->modelKeys());
        $this->assertCount(1, $actual);
        $this->assertNotContains($first->id, $actual->modelKeys());
    }

    public function test_corrupt_rows_are_repaired_without_reloading_valid_rows(): void
    {
        $first = Author::create(['name' => 'One']);
        Author::create(['name' => 'Two']);
        $query = fn() => Author::orderBy('id')->get();
        $expected = $query()->toArray();
        $this->cacheStore()->setRawForever($this->authorRowKey($first->id), 'corrupt');
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class, QueryCacheRepaired::class]);

        [$actual, $queries] = $this->observe($query);

        $this->assertSame($expected, $actual->toArray());
        $this->assertCount(1, $queries);
        $this->assertSame([$first->id], $queries[0]['bindings']);
        Event::assertDispatched(QueryCacheRepaired::class, fn($event) => $event->reason === 'corrupt_payload');
        Event::assertNotDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryCacheHit::class);
        $this->assertSame([], $this->cacheKeysMatching(':build:'));
        $this->assertWarmCacheHit($query);
    }

    public function test_duplicate_membership_ids_are_repaired_once_and_keep_their_positions(): void
    {
        $first = Author::create(['name' => 'One']);
        $second = Author::create(['name' => 'Two']);
        $query = fn() => Author::orderBy('id')->get();
        $query();
        $key = $this->cacheQueryKeysWithField('m')[0];
        $codec = app(MembershipCodec::class);
        $membership = $codec->decode($this->cacheStore()->readHashField($key, 'm'));
        $ids = ['i:' . $second->id, 'i:' . $first->id, 'i:' . $second->id];
        $this->writeCacheField($key, 'm', $codec->encode(
            $membership->epoch,
            $membership->generation,
            $ids,
            $membership->versions,
            $membership->tagVersion,
        ));
        $this->cacheStore()->delete($this->authorRowKey($second->id));

        [$actual, $queries] = $this->observe($query);

        $this->assertSame([$second->id, $first->id, $second->id], $actual->modelKeys());
        $this->assertCount(1, $queries);
        $this->assertSame([$second->id], $queries[0]['bindings']);
        $this->assertWarmCacheHit($query);
    }

    public function test_missing_database_rows_fall_back_to_the_original_query(): void
    {
        $first = Author::create(['name' => 'One']);
        $second = Author::create(['name' => 'Two']);
        $query = fn() => Author::orderBy('id')->get();
        $query();
        $this->cacheStore()->delete($this->authorRowKey($first->id));
        DB::table('authors')->where('id', $first->id)->delete();

        [$actual, $queries] = $this->observe($query);

        $this->assertSame([$second->id], $actual->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertSame([$first->id], $queries[0]['bindings']);
        $this->assertSame(Author::orderBy('id')->toSql(), $queries[1]['query']);
        $this->assertSame([], $this->cacheKeysMatching(':build:'));
        $this->assertWarmCacheHit($query);
    }

    #[DataProvider('counterChanges')]
    public function test_counter_changes_during_repair_prevent_serving_the_partial_result(string $counter): void
    {
        $author = Author::create(['name' => 'One']);
        $query = fn() => Author::orderBy('id')->tag('repair')->get();
        $query();
        $this->cacheStore()->delete($this->authorRowKey($author->id));
        $changed = false;
        DB::listen(function ($event) use (&$changed, $counter): void {
            if (!$changed && str_contains($event->sql, ' in (')) {
                $changed = true;

                match ($counter) {
                    'epoch' => $this->cacheStore()->increment($this->cacheKeys()->epoch()),
                    'generation' => $this->cacheManager()->invalidate(['authors']),
                    'tag' => $this->cacheManager()->flushTag('repair'),
                };
            }
        });
        Event::fake([QueryCacheMiss::class, QueryCacheRepaired::class]);

        [$actual, $queries] = $this->observe($query);

        $this->assertTrue($changed);
        $this->assertSame([$author->id], $actual->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertSame(Author::orderBy('id')->toSql(), $queries[1]['query']);
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryCacheRepaired::class);
        $this->assertSame([], $this->cacheKeysMatching(':build:'));
    }

    public static function counterChanges(): array
    {
        return [['epoch'], ['generation'], ['tag']];
    }

    public function test_string_primary_keys_are_decoded_for_missing_row_fetches(): void
    {
        UuidItem::create(['id' => 'a:{}', 'name' => 'One']);
        UuidItem::create(['id' => '42', 'name' => 'Two']);
        $query = fn() => UuidItem::orderBy('name')->get();
        $expected = $query()->toArray();
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'uuid_items');
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $token = (new PrimaryKeyMetadata('id', 'string'))->token('a:{}');
        $this->cacheStore()->delete($this->cacheKeys()->row($table, $generation, $token));

        [$actual, $queries] = $this->observe($query);

        $this->assertSame($expected, $actual->toArray());
        $this->assertCount(1, $queries);
        $this->assertSame(['a:{}'], $queries[0]['bindings']);
        $this->assertWarmCacheHit($query);
    }

    public function test_large_missing_sets_are_batched_without_reloading_cached_rows(): void
    {
        foreach (array_chunk(range(1, 905), 300) as $batch) {
            Author::insert(array_map(fn($id) => ['id' => $id, 'name' => 'Author ' . $id], $batch));
        }

        $query = fn() => Author::orderBy('id')->get();
        $expected = $query()->modelKeys();
        $rowKeys = $this->cacheKeysMatching(':r:g');
        $this->cacheStore()->delete(array_slice($rowKeys, 0, 901));

        [$actual, $queries] = $this->observe($query);

        $this->assertSame($expected, $actual->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertCount(900, $queries[0]['bindings']);
        $this->assertCount(1, $queries[1]['bindings']);
        $this->assertWarmCacheHit($query);
    }

    public function test_waiters_consume_repaired_rows_without_repeating_sql(): void
    {
        $author = Author::create(['name' => 'One']);
        $query = fn() => Author::orderBy('id')->get();
        $query();
        $rowKey = $this->authorRowKey($author->id);
        $payload = $this->cacheStore()->getRaw($rowKey);
        $this->cacheStore()->delete($rowKey);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $batch = hash('xxh128', TableIdentity::encodeFields(['i:' . $author->id]));
        $leases = app(BuildLeaseCoordinator::class);
        $owner = $leases->claimRepair($table, $generation, $batch);
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $published = false;
        $connection->listen(function (CommandExecuted $event) use (&$published, $owner, $rowKey, $payload, $leases): void {
            if (!$published && strtolower($event->command) === 'brpop') {
                $published = true;
                $this->cacheStore()->setRaw($rowKey, $payload, 60);
                $leases->release($owner);
            }
        });
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class, QueryCacheRepaired::class]);

        try {
            [$actual, $queries] = $this->observe($query);
        } finally {
            $connection->unsetEventDispatcher();
        }

        $this->assertTrue($published);
        $this->assertSame([$author->id], $actual->modelKeys());
        $this->assertSame([], $queries);
        Event::assertDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryCacheRepaired::class);
    }

    public function test_an_epoch_change_between_validation_and_publication_rejects_the_repair(): void
    {
        $author = Author::create(['name' => 'One']);
        $query = fn() => Author::orderBy('id')->get();
        $query();
        $this->cacheStore()->delete($this->authorRowKey($author->id));
        $connection = Redis::connection('normcache-test');
        $connection->setEventDispatcher($this->app->make('events'));
        $reads = 0;
        $changed = false;
        $connection->listen(function (CommandExecuted $event) use (&$reads, &$changed): void {
            if (strtolower($event->command) === 'mget' && ++$reads === 3) {
                $changed = true;
                $this->cacheStore()->increment($this->cacheKeys()->epoch());
            }
        });
        Event::fake([QueryCacheRepaired::class]);

        try {
            [$actual, $queries] = $this->observe($query);
        } finally {
            $connection->unsetEventDispatcher();
        }

        $this->assertTrue($changed);
        $this->assertSame([$author->id], $actual->modelKeys());
        $this->assertCount(2, $queries);
        Event::assertNotDispatched(QueryCacheRepaired::class);
    }

    public function test_a_repair_sql_failure_releases_the_lease_and_uses_the_original_query(): void
    {
        $author = Author::create(['name' => 'One']);
        $query = fn() => Author::orderBy('id')->get();
        $query();
        $this->cacheStore()->delete($this->authorRowKey($author->id));
        $failed = false;
        DB::connection()->beforeExecuting(function (string $sql) use (&$failed): void {
            if (!$failed && str_contains($sql, ' in (')) {
                $failed = true;
                throw new \RuntimeException('The repair query could not execute.');
            }
        });

        $this->assertSame([$author->id], $query()->modelKeys());
        $this->assertTrue($failed);
        $this->assertSame([], $this->cacheKeysMatching(':build:'));
        $this->assertWarmCacheHit($query);
    }

    public function test_lost_repair_lease_prevents_publication_and_preserves_the_new_owner(): void
    {
        $author = Author::create(['name' => 'One']);
        $query = fn() => Author::orderBy('id')->get();
        $query();
        $rowKey = $this->authorRowKey($author->id);
        $this->cacheStore()->delete($rowKey);
        $leaseKey = null;
        $newOwner = str_repeat('f', 32);
        $unpublished = false;
        DB::listen(function ($event) use (&$leaseKey, &$unpublished, $rowKey, $newOwner): void {
            if (str_contains($event->sql, ' in (')) {
                $leaseKey = $this->cacheKeysMatching(':build:x:')[0];
                $this->cacheStore()->setRaw($leaseKey, $newOwner, 5);
            } elseif ($leaseKey !== null) {
                $unpublished = $this->cacheStore()->getRaw($rowKey) === null;
            }
        });
        Event::fake([QueryCacheRepaired::class]);

        [$actual, $queries] = $this->observe($query);

        $this->assertSame([$author->id], $actual->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertTrue($unpublished);
        $this->assertSame($newOwner, $this->cacheStore()->getRaw($leaseKey));
        Event::assertNotDispatched(QueryCacheRepaired::class);
    }

    /** @return array{mixed, array} */
    private function observe(callable $query): array
    {
        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            return [$query(), DB::getQueryLog()];
        } finally {
            DB::disableQueryLog();
        }
    }

    private function authorRowKey(int $id): string
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';

        return $this->cacheKeys()->row($table, $generation, 'i:' . $id);
    }
}
