<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class KnownPrimaryKeyColdFillTest extends TestCase
{
    /** @var list<int> */
    private array $ids = [];

    protected function setUp(): void
    {
        parent::setUp();
        $author = Author::query()->create(['name' => 'Known key author']);
        for ($index = 1; $index <= 5; $index++) {
            $this->ids[] = (int) RawPost::query()->toBase()->insertGetId([
                'title' => "Known {$index}", 'views' => $index, 'published' => true,
                'author_id' => $author->getKey(), 'created_at' => now(), 'updated_at' => now(),
            ]);
        }
    }

    public function test_cold_fill_uses_one_row_select_without_id_discovery(): void
    {
        [$rows, $queries] = $this->observe(fn() => $this->raw([0, 1])->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
        $this->assertStringContainsString(' in (', strtolower($queries[0]['query']));
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);
    }

    public function test_eighty_percent_reuse_reads_only_missing_candidate_rows(): void
    {
        $this->raw([0, 1, 2, 3])->get();

        [$rows, $queries] = $this->observe(fn() => $this->raw([0, 1, 2, 3, 4])->get());

        $this->assertSame(array_slice($this->ids, 0, 5), $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[4]]);
    }

    public function test_full_row_reuse_needs_no_sql_for_a_new_membership(): void
    {
        $this->raw([0, 1, 2, 3])->get();

        [$rows, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[3], $this->ids[1]])->orderBy('id', 'desc')->get());

        $this->assertSame([$this->ids[3], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertSame([], $queries);
    }

    #[DataProvider('bigIntegerDirections')]
    public function test_big_integer_candidates_keep_exact_order_with_duplicates(string $direction): void
    {
        $this->assertBigIntegerCandidateOrderMatchesDatabase($direction);
    }

    /** @return array<string, array{string}> */
    public static function bigIntegerDirections(): array
    {
        return ['ascending' => ['asc'], 'descending' => ['desc']];
    }

    public function test_absent_candidate_ids_are_omitted_from_the_cold_result(): void
    {
        [$rows, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], 999999999, $this->ids[1]])->orderBy('id')->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1], 999999999]);
    }

    public function test_table_alias_and_qualified_columns_use_candidate_fill(): void
    {
        [$rows, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->from('posts as p')
            ->select('p.*')
            ->whereIn('p.id', [$this->ids[0], $this->ids[1]])
            ->orderBy('p.id')
            ->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
    }

    public function test_unordered_and_limited_queries_keep_id_discovery(): void
    {
        foreach ([
            RawPost::query()->toBase()->whereIn('id', [$this->ids[0], $this->ids[1]]),
            RawPost::query()->toBase()->whereIn('id', [$this->ids[0], $this->ids[1]])->orderBy('id')->offset(1)->limit(2),
            RawPost::query()->toBase()->whereIn('id', [$this->ids[0], $this->ids[1]])->orderBy('id')->limit(2),
        ] as $query) {
            $this->cacheManager()->invalidate(['posts']);
            $native = (clone $query)->withoutCache()->get();
            [$actual, $queries] = $this->observe(fn() => $query->get());
            $this->assertSame($native->pluck('id')->all(), $actual->pluck('id')->all());
            $this->assertCount(2, $queries);
            $this->assertIdOnlyDiscovery($queries[0]);
        }
    }

    public function test_tagged_candidates_use_candidate_fill_without_id_discovery(): void
    {
        [$rows, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->tag('known-candidate-tag')
            ->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);

        [$warmRows, $warmQueries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->tag('known-candidate-tag')
            ->get());
        $this->assertSame([$this->ids[0], $this->ids[1]], $warmRows->pluck('id')->all());
        $this->assertSame([], $warmQueries);

        $this->cacheManager()->flushTag('known-candidate-tag');

        [$reloadedRows, $reloadedQueries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->tag('known-candidate-tag')
            ->get());
        $this->assertSame([$this->ids[0], $this->ids[1]], $reloadedRows->pluck('id')->all());
        $this->assertSame([], $reloadedQueries);

        $this->cacheManager()->invalidate(['posts']);
        [$coldRows, $coldQueries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->tag('known-candidate-tag')
            ->get());
        $this->assertSame([$this->ids[0], $this->ids[1]], $coldRows->pluck('id')->all());
        $this->assertCount(1, $coldQueries);
        $this->assertIntegerPrimaryKeyBatch($coldQueries[0], [$this->ids[0], $this->ids[1]]);
    }

    public function test_custom_ttl_candidates_use_candidate_fill_without_id_discovery(): void
    {
        [$rows, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->ttl(120)
            ->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);
        $this->assertRequestedQueryEntryTtl($this->cacheQueryKeysWithField('m'), 120);
    }

    public function test_tagged_and_custom_ttl_combined(): void
    {
        [$rows, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->tag('combined-tag')
            ->ttl(120)
            ->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);
        $this->assertRequestedQueryEntryTtl($this->cacheQueryKeysWithField('m'), 120);
    }

    public function test_custom_ttl_membership_reuses_rows_without_extending_row_ttl(): void
    {
        $this->raw([0, 1])->get();
        $base = RawPost::query()->toBase();
        $table = app(TableIdentityResolver::class)->resolve($base->getConnection(), $base->from);
        $this->assertNotNull($table);
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $rowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $this->ids[0]);
        $redis = Redis::connection('normcache-test');
        $this->assertTrue((bool) $redis->expire($rowKey, 30));
        $previousMembershipKeys = $this->cacheQueryKeysWithField('m');

        [$rows, $queries] = $this->observe(fn() => $this->raw([0, 1])->tag('short-row-ttl')->ttl(120)->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertSame([], $queries);
        $this->assertGreaterThan(0, (int) $redis->ttl($rowKey));
        $this->assertLessThanOrEqual(30, (int) $redis->ttl($rowKey));
        $this->assertRequestedQueryEntryTtl(
            array_values(array_diff($this->cacheQueryKeysWithField('m'), $previousMembershipKeys)),
            120,
        );
    }

    public function test_tag_change_during_candidate_fetch_does_not_publish_old_tag_entry(): void
    {
        $flushed = false;
        DB::listen(function (QueryExecuted $event) use (&$flushed): void {
            if (!$flushed && str_contains(strtolower($event->sql), ' in (')) {
                $flushed = true;
                $this->cacheManager()->flushTag('candidate-race-tag');
            }
        });

        [$rows, $queries] = $this->observe(fn() => $this->raw([0, 1])->tag('candidate-race-tag')->get());

        $this->assertTrue($flushed);
        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(2, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);
        $this->assertSame($this->raw([0, 1])->toSql(), $queries[1]['query']);
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
        $this->assertSame([], $this->cacheQueryKeysWithField('r'));

        [$newRows, $newQueries] = $this->observe(fn() => $this->raw([0, 1])->tag('candidate-race-tag')->get());
        $this->assertSame([$this->ids[0], $this->ids[1]], $newRows->pluck('id')->all());
        $this->assertCount(1, $newQueries);
        $this->assertIntegerPrimaryKeyBatch($newQueries[0], [$this->ids[0], $this->ids[1]]);
        $this->assertNotEmpty($this->cacheQueryKeysWithField('m'));

        [$warmRows, $warmQueries] = $this->observe(fn() => $this->raw([0, 1])->tag('candidate-race-tag')->get());
        $this->assertSame([$this->ids[0], $this->ids[1]], $warmRows->pluck('id')->all());
        $this->assertSame([], $warmQueries);
    }

    public function test_context_candidates_keep_the_result_route(): void
    {
        [, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->orderBy('id')
            ->cacheContext('known-candidate-context')
            ->get());

        $this->assertCount(1, $queries);
        $this->assertStringStartsWith('select * from ', strtolower($queries[0]['query']));
    }

    public function test_extra_predicate_uses_the_existing_id_first_route(): void
    {
        [, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])
            ->where('published', true)->orderBy('id')->get());

        $this->assertCount(2, $queries);
        $this->assertIdOnlyDiscovery($queries[0]);
    }

    public function test_string_primary_keys_keep_the_existing_route(): void
    {
        UuidItem::query()->create(['id' => 'known-id', 'name' => 'Known']);

        [, $queries] = $this->observe(fn() => UuidItem::query()->whereIn('id', ['known-id'])->orderBy('id')->get());

        $this->assertCount(2, $queries);
        $this->assertIdOnlyDiscovery($queries[0]);
        $this->assertMatchesRegularExpression('/\\bin\\s*\\(\\?\\)/i', $queries[1]['query']);
        $this->assertSame(['known-id'], $queries[1]['bindings']);
    }

    public function test_overflow_integer_keys_fall_back_without_integer_truncation(): void
    {
        [, $queries] = $this->observe(fn() => RawPost::query()->toBase()
            ->whereIn('id', ['9223372036854775808'])->orderBy('id')->get());

        $this->assertCount(1, $queries);
        $this->assertIdOnlyDiscovery($queries[0]);
        $this->assertContains('9223372036854775808', array_map('strval', $queries[0]['bindings']));
    }

    public function test_soft_delete_visibility_is_applied_to_reused_and_missing_candidates(): void
    {
        $post = Post::query()->findOrFail($this->ids[0]);
        $post->delete();
        Post::withTrashed()->where('id', $this->ids[0])->orderBy('id')->get();

        [$rows, $queries] = $this->observe(fn() => Post::query()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])->orderBy('id')->get());

        $this->assertSame([$this->ids[1]], $rows->modelKeys());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[1]]);
    }

    public function test_only_trashed_visibility_is_applied_to_database_candidates(): void
    {
        Post::query()->findOrFail($this->ids[0])->delete();

        [$rows, $queries] = $this->observe(fn() => Post::onlyTrashed()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])->orderBy('id')->get());

        $this->assertSame([$this->ids[0]], $rows->modelKeys());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);
    }

    public function test_cached_rows_without_the_soft_delete_column_are_refetched(): void
    {
        $known = Post::withTrashed()->whereIn('id', [$this->ids[0]])
            ->orderBy('id')->toBase()->get();
        $base = Post::withTrashed()->toBase();
        $table = app(TableIdentityResolver::class)->resolve($base->getConnection(), $base->from);
        $this->assertNotNull($table);
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $epoch = $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0';
        $rowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $this->ids[0]);
        $incomplete = clone $known[0];
        unset($incomplete->deleted_at);
        $this->cacheStore()->setRawForever(
            $rowKey,
            app(RawResultCodec::class)->encodeRow($incomplete, $epoch),
        );

        [$rows, $queries] = $this->observe(fn() => Post::withTrashed()
            ->whereIn('id', [$this->ids[0], $this->ids[1]])->orderBy('id')->get());

        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->modelKeys());
        $this->assertCount(1, $queries);
        $this->assertIntegerPrimaryKeyBatch($queries[0], [$this->ids[0], $this->ids[1]]);
    }

    public function test_state_change_during_candidate_fill_falls_back_to_the_original_query(): void
    {
        $changed = false;
        DB::listen(function (QueryExecuted $event) use (&$changed): void {
            if (!$changed && str_contains(strtolower($event->sql), ' in (')) {
                $changed = true;
                $this->cacheStore()->increment($this->cacheKeys()->epoch());
            }
        });

        [$rows, $queries] = $this->observe(fn() => $this->raw([0, 1])->get());

        $this->assertTrue($changed);
        $this->assertSame([$this->ids[0], $this->ids[1]], $rows->pluck('id')->all());
        $this->assertCount(2, $queries);
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
    }

    public function test_rows_with_the_wrong_epoch_or_primary_key_token_are_fetched_again(): void
    {
        $query = $this->raw([0])->get();
        $base = RawPost::query()->toBase();
        $table = app(TableIdentityResolver::class)->resolve($base->getConnection(), $base->from);
        $this->assertNotNull($table);
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $epoch = $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0';
        $rowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $this->ids[0]);
        $other = RawPost::query()->toBase()->withoutCache()->where('id', $this->ids[1])->first();
        $this->assertNotNull($other);
        $codec = app(RawResultCodec::class);

        $this->cacheStore()->setRawForever($rowKey, $codec->encodeRow($query[0], 'wrong-epoch'));
        [, $epochQueries] = $this->observe(fn() => $this->raw([0, 2])->get());
        $this->assertCount(1, $epochQueries);
        $this->assertIntegerPrimaryKeyBatch($epochQueries[0], [$this->ids[0], $this->ids[2]]);

        $this->cacheStore()->setRawForever($rowKey, $codec->encodeRow($other, $epoch));
        [$rows, $tokenQueries] = $this->observe(fn() => $this->raw([0, 3])->get());
        $this->assertSame([$this->ids[0], $this->ids[3]], $rows->pluck('id')->all());
        $this->assertCount(1, $tokenQueries);
        $this->assertIntegerPrimaryKeyBatch($tokenQueries[0], [$this->ids[0], $this->ids[3]]);
    }

    private function raw(array $indexes): Builder
    {
        return RawPost::query()->toBase()
            ->whereIn('id', array_map(fn(int $index): int => $this->ids[$index], $indexes))
            ->orderBy('id');
    }

    /** @param list<string> $entries */
    private function assertRequestedQueryEntryTtl(array $entries, int $seconds): void
    {
        $this->assertCount(1, $entries);
        $redis = Redis::connection('normcache-test');
        $ttl = (int) $redis->ttl($entries[0]);
        $this->assertGreaterThan($seconds - 10, $ttl);
        $this->assertLessThanOrEqual($seconds, $ttl);
    }

    /** @param array{query: string, bindings: array} $query */
    private function assertIdOnlyDiscovery(array $query): void
    {
        $wrappedId = strtolower(DB::connection()->getQueryGrammar()->wrap('id'));
        $this->assertStringStartsWith('select ' . $wrappedId . ' from ', strtolower($query['query']));
    }

    private function assertBigIntegerCandidateOrderMatchesDatabase(string $direction): void
    {
        $authorId = Author::query()->toBase()->value('id');
        $lower = 9007199254740992;
        $upper = 9007199254740993;
        DB::table('posts')->insert([
            [
                'id' => $lower,
                'title' => 'Big integer lower',
                'views' => 1,
                'published' => true,
                'author_id' => $authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ],
            [
                'id' => $upper,
                'title' => 'Big integer upper',
                'views' => 2,
                'published' => true,
                'author_id' => $authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ],
        ]);
        $candidateIds = [$upper, $lower, $upper, $lower];
        $nativeRows = RawPost::query()->toBase()
            ->whereIn('id', $candidateIds)
            ->orderBy('id', $direction)
            ->withoutCache()
            ->get();
        $cachedRows = RawPost::query()->toBase()
            ->whereIn('id', $candidateIds)
            ->orderBy('id', $direction)
            ->get();

        $toIds = static fn(iterable $rows): array => array_map(
            static fn(object $row): string => (string) $row->id,
            is_array($rows) ? $rows : $rows->all(),
        );
        $this->assertSame($toIds($nativeRows), $toIds($cachedRows));
        $this->assertSame(
            $direction === 'asc' ? [(string) $lower, (string) $upper] : [(string) $upper, (string) $lower],
            $toIds($cachedRows),
        );
    }

    /** @return array{mixed, list<array{query: string, bindings: array}>} */
    private function observe(callable $callback): array
    {
        DB::flushQueryLog();
        DB::enableQueryLog();
        try {
            $result = $callback();
            $queries = DB::getQueryLog();
        } finally {
            DB::disableQueryLog();
        }

        return [$result, $queries];
    }
}
