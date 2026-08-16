<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Payload\ChangeRecordCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\Fixtures\Models\VolatilePost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheState;
use NormCache\Values\OverlayAdmission;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class MembershipRevalidationTest extends TestCase
{
    private int $authorId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->authorId = (int) Author::query()->create(['name' => 'Author'])->getKey();
    }

    /** Mirrors MembershipRevalidator::MAX_VERSION_GAP. */
    private const MAX_VERSION_GAP = 128;

    private function enableRevalidation(): void
    {
        config()->set('normcache.revalidation', true);

        $this->app->forgetInstance(CacheConfig::class);
        $this->app->forgetScopedInstances();
    }

    private function seedPosts(int $count): void
    {
        $rows = [];

        for ($index = 1; $index <= $count; $index++) {
            $rows[] = [
                'title' => 'Post ' . $index,
                'views' => $index,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        RawPost::query()->toBase()->insert($rows);
    }

    /** @return list<array<string, mixed>> */
    private function captureQueries(callable $callback): array
    {
        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            $callback();
        } finally {
            DB::disableQueryLog();
        }

        return DB::getQueryLog();
    }

    private function assertServedWithoutSql(callable $callback): void
    {
        $this->assertSame(
            [],
            $this->captureQueries($callback),
            'expected the read to be served entirely from NormCache',
        );
    }

    private function assertRevalidated(callable $callback): void
    {
        $dispatcher = Event::getFacadeRoot();
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class, QueryCacheRepaired::class]);

        try {
            $callback();
            Event::assertNotDispatched(QueryCacheMiss::class);
            Event::assertDispatched(QueryCacheRepaired::class);
        } finally {
            Event::swap($dispatcher);
        }
    }

    private function assertNotRevalidated(callable $callback): void
    {
        $dispatcher = Event::getFacadeRoot();
        Event::fake([QueryCacheHit::class, QueryCacheMiss::class, QueryCacheRepaired::class]);

        try {
            $callback();
            Event::assertDispatched(QueryCacheMiss::class);
            Event::assertNotDispatched(QueryCacheRepaired::class);
        } finally {
            Event::swap($dispatcher);
        }
    }

    private function forgetChangeRecord(string $table, string $version): void
    {
        $this->cacheStore()->delete(
            $this->cacheKeys()->changeRecord($this->tableIdentity($table), $version),
        );
    }

    private function tableIdentity(string $table): TableIdentity
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $table);

        $this->assertNotNull($identity);

        return $identity;
    }

    private function currentVersion(string $table): string
    {
        return (string) ($this->cacheStore()->getRaw(
            $this->cacheKeys()->version($this->tableIdentity($table)),
        ) ?? '0');
    }

    /** @param list<string> $columns */
    private function forgeChangeRecord(
        string $table,
        string $version,
        string $mutation,
        array $columns,
        bool $precise,
    ): void {
        $this->cacheStore()->setRawForever(
            $this->cacheKeys()->changeRecord($this->tableIdentity($table), $version),
            $this->app->make(ChangeRecordCodec::class)->encode($mutation, $columns, $precise),
        );
    }

    private function deleteAllCanonicalRows(): void
    {
        $rows = array_values(array_filter(
            $this->cacheKeysMatching(':r:g'),
            static fn(string $key): bool => !str_contains($key, ':build:'),
        ));

        $this->assertNotSame([], $rows, 'expected canonical rows to exist');

        $this->cacheStore()->delete($rows);
    }

    public function test_an_update_to_a_non_predicate_column_keeps_the_membership(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertRevalidated(function () use (&$queries): void {
            $queries = $this->captureQueries(fn() => RawPost::query()->toBase()->get());
        });

        $this->assertCount(1, $queries, 'only the repair of row 7');
        $this->assertStringContainsString('in (?)', $queries[0]['query']);
    }

    public function test_the_revalidated_read_returns_the_new_value(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $rows = collect(RawPost::query()->toBase()->get());

        $this->assertSame('changed', $rows->firstWhere('id', 7)->title);
        $this->assertCount(50, $rows);
    }

    public function test_the_read_after_a_revalidation_is_an_ordinary_hit(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);
        RawPost::query()->toBase()->get();

        $this->assertServedWithoutSql(fn() => RawPost::query()->toBase()->get());
    }

    public function test_revalidation_is_on_by_default(): void
    {
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_revalidation_can_be_turned_off(): void
    {
        config()->set('normcache.revalidation', false);
        $this->app->forgetInstance(CacheConfig::class);
        $this->app->forgetScopedInstances();

        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_an_update_to_a_predicate_column_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->where('published', true)->get();

        RawPost::query()->toBase()->where('id', 7)->update(['published' => false]);

        $this->assertNotRevalidated(
            fn() => RawPost::query()->toBase()->where('published', true)->get(),
        );
        $this->assertCount(49, RawPost::query()->toBase()->where('published', true)->get());
    }

    public function test_a_volatile_column_changed_by_a_trigger_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(3);
        DB::statement(<<<'SQL'
            CREATE TRIGGER posts_title_unpublishes
            AFTER UPDATE OF title ON posts
            FOR EACH ROW
            WHEN NEW.title = 'hidden'
            BEGIN
                UPDATE posts SET published = 0 WHERE id = NEW.id;
            END
            SQL);

        $read = fn() => VolatilePost::query()->toBase()
            ->where('published', true)
            ->orderBy('id')
            ->get();

        $read();
        VolatilePost::query()->toBase()->where('id', 1)->update(['title' => 'hidden']);

        $this->assertNotRevalidated($read);
        $this->assertEquals(
            VolatilePost::query()->toBase()
                ->where('published', true)
                ->orderBy('id')
                ->internal()
                ->get(),
            $read(),
        );
    }

    public function test_a_query_guarding_no_volatile_column_still_revalidates(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);

        $read = fn() => VolatilePost::query()->toBase()->orderBy('views')->get();

        $read();
        VolatilePost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertRevalidated($read);
        $this->assertSame('changed', $read()->firstWhere('id', 7)->title);
    }

    public function test_a_volatile_column_is_recorded_alongside_the_assigned_columns(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(3);

        VolatilePost::query()->toBase()->where('id', 1)->update(['title' => 'changed']);

        $record = $this->app->make(ChangeRecordCodec::class)->decode(
            (string) $this->cacheStore()->getRaw(
                $this->cacheKeys()->changeRecord(
                    $this->tableIdentity('posts'),
                    $this->currentVersion('posts'),
                ),
            ),
        );

        $this->assertTrue($record->valid);
        $this->assertEqualsCanonicalizing(
            ['title', 'published', 'title_length'],
            $record->columns,
        );
    }

    public function test_integer_in_raw_predicates_can_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(5);

        $read = fn() => RawPost::query()->toBase()
            ->whereIntegerInRaw('id', [1, 2, 3])
            ->orderBy('id')
            ->get();

        $read();
        RawPost::query()->toBase()->where('id', 2)->update(['title' => 'changed']);

        $this->assertRevalidated($read);
        $this->assertSame('changed', $read()->firstWhere('id', 2)->title);
    }

    public function test_a_volatile_generated_predicate_column_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        DB::statement(
            'alter table posts add column title_length integer generated always as (length(title)) virtual',
        );
        $this->seedPosts(5);

        $read = fn() => VolatilePost::query()->toBase()
            ->where('title_length', '>', 4)
            ->orderBy('id')
            ->get();

        $read();
        VolatilePost::query()->toBase()->where('id', 1)->update(['title' => 'x']);

        $this->assertNotRevalidated($read);
        $this->assertCount(4, $read());
    }

    public function test_an_undeclared_generated_predicate_column_revalidates_wrongly(): void
    {
        $this->enableRevalidation();
        DB::statement(
            'alter table posts add column title_length integer generated always as (length(title)) virtual',
        );
        $this->seedPosts(5);

        $read = fn() => RawPost::query()->toBase()
            ->where('title_length', '>', 4)
            ->orderBy('id')
            ->get();

        $read();
        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);

        $this->assertRevalidated($read);
        $this->assertCount(5, $read(), 'the stale membership still holds the shortened row');
        $this->assertCount(
            4,
            RawPost::query()->toBase()
                ->where('title_length', '>', 4)
                ->orderBy('id')
                ->internal()
                ->get(),
        );
    }

    public function test_an_update_to_an_order_column_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->orderBy('views')->get();

        RawPost::query()->toBase()->where('id', 7)->update(['views' => 999]);

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->orderBy('views')->get());

        $rows = collect(RawPost::query()->toBase()->orderBy('views')->get());
        $this->assertSame(999, (int) $rows->last()->views);
    }

    public function test_an_insert_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->insert([
            'title' => 'new',
            'views' => 0,
            'published' => true,
            'author_id' => $this->authorId,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
        $this->assertCount(51, RawPost::query()->toBase()->get());
    }

    public function test_a_delete_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('id', 7)->delete();

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
        $this->assertCount(49, RawPost::query()->toBase()->get());
    }

    public function test_a_non_precise_update_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('title', 'like', 'Post 1%')->update(['title' => 'x']);

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_a_missing_change_record_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();
        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->forgetChangeRecord('posts', '2');

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_a_record_naming_a_non_update_mutation_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();
        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->forgeChangeRecord(
            'posts',
            $this->currentVersion('posts'),
            mutation: 'delete',
            columns: ['title'],
            precise: true,
        );

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_a_non_precise_record_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();
        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->forgeChangeRecord(
            'posts',
            $this->currentVersion('posts'),
            mutation: 'update',
            columns: ['title'],
            precise: false,
        );

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_a_corrupt_change_record_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();
        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->cacheStore()->setRawForever(
            $this->cacheKeys()->changeRecord(
                $this->tableIdentity('posts'),
                $this->currentVersion('posts'),
            ),
            'not-a-payload',
        );

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_a_version_gap_beyond_the_cap_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        // The seed insert already consumed one version.
        foreach (range(1, self::MAX_VERSION_GAP + 1) as $pass) {
            RawPost::query()->toBase()->where('id', 7)->update(['title' => "t{$pass}"]);
        }

        $this->assertNotRevalidated(fn() => RawPost::query()->toBase()->get());
    }

    public function test_a_version_gap_within_the_cap_still_revalidates(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        foreach ([1, 2, 3] as $id) {
            RawPost::query()->toBase()->where('id', $id)->update(['title' => "t{$id}"]);
        }

        $this->assertRevalidated(function () use (&$queries): void {
            $queries = $this->captureQueries(fn() => RawPost::query()->toBase()->get());
        });

        $this->assertCount(1, $queries, 'only the repair of rows 1-3');

        $rows = collect(RawPost::query()->toBase()->get());
        $this->assertSame('t1', $rows->firstWhere('id', 1)->title);
        $this->assertSame('t3', $rows->firstWhere('id', 3)->title);
    }

    public function test_an_unparseable_predicate_does_not_revalidate(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->whereRaw('length(title) > ?', [1])->get();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertNotRevalidated(
            fn() => RawPost::query()->toBase()->whereRaw('length(title) > ?', [1])->get(),
        );
    }

    public function test_revalidation_does_not_leave_a_stale_overlay(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();
        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        RawPost::query()->toBase()->get();

        // Force the next read through the overlay.
        $this->deleteAllCanonicalRows();

        $rows = collect(RawPost::query()->toBase()->get());
        $this->assertSame('changed', $rows->firstWhere('id', 7)->title);
    }

    public function test_a_revalidation_without_an_overlay_drops_the_stale_one(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        $this->assertNotSame(
            [],
            $this->cacheQueryKeysWithField('r'),
            'expected the first read to publish an overlay',
        );

        RawPost::query()->toBase()->where('id', 7)->update([
            'metadata' => json_encode(['blob' => str_repeat('y', 200_000)]),
        ]);

        $this->assertRevalidated(fn() => RawPost::query()->toBase()->get());

        $this->assertSame(
            [],
            $this->cacheQueryKeysWithField('r'),
            'the pre-update overlay must not survive a re-stamp that writes no overlay',
        );

        $rows = collect(RawPost::query()->toBase()->get());
        $this->assertCount(50, $rows);
        $this->assertStringContainsString('yyy', (string) $rows->firstWhere('id', 7)->metadata);
    }

    public function test_a_rejected_restamp_leaves_the_overlay_alone(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(3);
        RawPost::query()->toBase()->get();

        $entryKeys = $this->cacheQueryKeysWithField('r');
        $this->assertNotSame([], $entryKeys, 'expected the first read to publish an overlay');
        $key = $entryKeys[0];

        $overlay = $this->cacheStore()->readHashField($key, 'r');
        $membership = $this->cacheStore()->readHashField($key, 'm');

        $identity = $this->tableIdentity('posts');
        $query = RawPost::query()->toBase();
        $primaryKey = $query->primaryKey();
        $this->assertNotNull($primaryKey);

        $this->app->make(QueryEntryRepository::class)->restampCanonical(
            $query,
            QueryPlan::canonical($identity, [$identity], $primaryKey),
            new CacheState(
                key: $key,
                epoch: '0',
                // Unreachable table version forces rejection.
                version: '99999',
                generation: '0',
                versions: [],
                tag: null,
                tagKey: null,
            ),
            [(object) ['id' => 1], (object) ['id' => 2], (object) ['id' => 3]],
            OverlayAdmission::rejected(),
        );

        $this->assertSame($membership, $this->cacheStore()->readHashField($key, 'm'));
        $this->assertSame($overlay, $this->cacheStore()->readHashField($key, 'r'));
    }

    /** @return callable(): mixed */
    private function projectedRead(): callable
    {
        return fn() => RawPost::query()->toBase()->select('id', 'title')->orderBy('views')->get();
    }

    /** @return callable(): mixed */
    private function wildcardRead(): callable
    {
        return fn() => RawPost::query()->toBase()->orderBy('views')->get();
    }

    public function test_a_projected_query_revalidates_through_the_canonical_membership(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);

        ($this->wildcardRead())();
        ($this->projectedRead())();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertRevalidated($this->projectedRead());
    }

    public function test_a_revalidated_projection_reflects_an_updated_projected_column(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);

        ($this->wildcardRead())();
        ($this->projectedRead())();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $rows = collect(($this->projectedRead())());

        $this->assertSame('changed', $rows->firstWhere('id', 7)->title);
        $this->assertCount(50, $rows);
        $this->assertSame(['id', 'title'], array_keys((array) $rows->first()));
    }

    public function test_a_projected_query_still_does_not_revalidate_a_predicate_column(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);

        ($this->wildcardRead())();
        ($this->projectedRead())();

        RawPost::query()->toBase()->where('id', 7)->update(['views' => 999]);

        $this->assertNotRevalidated($this->projectedRead());
    }

    public function test_a_projected_query_without_a_canonical_sibling_still_misses(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);

        ($this->projectedRead())();

        RawPost::query()->toBase()->where('id', 7)->update(['title' => 'changed']);

        $this->assertNotRevalidated($this->projectedRead());
    }

    public function test_a_stale_membership_is_still_rejected_without_approval(): void
    {
        $this->enableRevalidation();
        $this->seedPosts(50);
        RawPost::query()->toBase()->get();

        RawPost::query()->toBase()->where('published', true)->get();
        RawPost::query()->toBase()->where('id', 7)->update(['published' => false]);

        $this->assertNotRevalidated(
            fn() => RawPost::query()->toBase()->where('published', true)->get(),
        );
    }
}
