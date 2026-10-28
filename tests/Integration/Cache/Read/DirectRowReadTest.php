<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\QueryEntryRepository;
use NormCache\Cache\QueryHashResolver;
use NormCache\Enums\ReadOutcome;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheRead;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class DirectRowReadTest extends TestCase
{
    public function test_a_missing_row_reports_a_miss_with_the_current_epoch_and_no_reason(): void
    {
        $epoch = $this->app->make(CacheRuntime::class)->epoch();
        $cached = $this->read($this->plan('i:404'));

        $this->assertSame(ReadOutcome::MISS, $cached->outcome);
        $this->assertSame([], $cached->rows);
        $this->assertSame($epoch, $cached->state->epoch);
        $this->assertNull($cached->reason);
    }

    public function test_a_corrupt_payload_is_reported_as_such(): void
    {
        $plan = $this->plan('i:1');
        $generation = $this->read($plan)->state->generation;

        $this->cacheStore()->setRawForever(
            $this->cacheKeys()->row($plan->root, $generation, 'i:1'),
            'not-a-payload',
        );

        $cached = $this->read($plan);

        $this->assertSame(ReadOutcome::MISS, $cached->outcome);
        $this->assertSame([], $cached->rows);
        $this->assertSame('corrupt_payload', $cached->reason);
    }

    public function test_soft_delete_visibility_filters_by_plan_mode(): void
    {
        $live = ['id' => 1, 'deleted_at' => null];
        $trashed = ['id' => 2, 'deleted_at' => '2026-01-01 00:00:00'];
        $this->seedRow($this->plan('i:1'), $live);
        $this->seedRow($this->plan('i:2'), $trashed);

        foreach ([
            ['i:1', 'default', [$live]],
            ['i:2', 'default', []],
            ['i:2', 'only', [$trashed]],
            ['i:1', 'only', []],
            ['i:2', 'with', [$trashed]],
        ] as [$token, $mode, $expected]) {
            $cached = $this->read($this->plan($token, $mode));

            $this->assertSame(ReadOutcome::HIT, $cached->outcome);
            $this->assertSame($expected, array_map(static fn(\stdClass $row): array => (array) $row, $cached->rows));
        }
    }

    public function test_visibility_reports_a_row_missing_its_deleted_at_column(): void
    {
        $plan = $this->plan('i:1', 'default');
        $this->seedRow($plan, ['id' => 1]);

        $cached = $this->read($plan);

        $this->assertSame(ReadOutcome::MISS, $cached->outcome);
        $this->assertSame([], $cached->rows);
        $this->assertSame('corrupt_payload', $cached->reason);
    }

    public function test_warm_direct_row_does_not_compute_the_query_hash(): void
    {
        $plan = $this->plan('i:7');
        $this->seedRow($plan, ['id' => 7]);

        $cached = $this->app->make(QueryEntryRepository::class)->read(
            RawPost::query()->toBase(),
            $plan,
            'u',
            new QueryHashResolver(static fn(): string => throw new \LogicException('query hash was evaluated')),
        );

        $this->assertNotNull($cached);
        $this->assertTrue($cached->served());
        $this->assertSame(7, $cached->rows[0]->id);
    }

    public function test_row_state_is_scoped_to_the_generation_and_token(): void
    {
        $plan = $this->plan('i:7');
        $this->cacheStore()->setRawForever($this->cacheKeys()->generation($plan->root), '3');
        $this->cacheStore()->setRawForever($this->cacheKeys()->epoch(), '9');
        $this->app->make(CacheRuntime::class)->forgetEpoch();
        $this->seedRow($plan, ['id' => 7]);

        $cached = $this->read($plan);

        $this->assertSame(ReadOutcome::HIT, $cached->outcome);
        $this->assertSame([['id' => 7]], array_map(static fn(\stdClass $row): array => (array) $row, $cached->rows));
        $this->assertSame($this->cacheKeys()->row($plan->root, '3', 'i:7'), $cached->state->key);
        $this->assertSame('3', $cached->state->generation);
        $this->assertSame('9', $cached->state->epoch);
        $this->assertSame('0', $cached->state->version);
    }

    private function read(QueryPlan $plan): CacheRead
    {
        $cached = $this->app->make(QueryEntryRepository::class)->read(
            RawPost::query()->toBase(),
            $plan,
            'u',
            new QueryHashResolver(static fn(): string => 'direct-test'),
        );
        $this->assertNotNull($cached);

        return $cached;
    }

    private function seedRow(QueryPlan $plan, array $row): void
    {
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($plan->root)) ?? '0';
        $payload = $this->app->make(RawResultCodec::class)->encodeRow(
            (object) $row,
            $this->app->make(CacheRuntime::class)->epoch(),
        );
        $this->assertNotNull($payload);
        $this->cacheStore()->setRawForever(
            $this->cacheKeys()->row($plan->root, $generation, $plan->primaryKeyToken),
            $payload,
        );
    }

    private function plan(string $token, ?string $softDeleteMode = null): QueryPlan
    {
        $root = $this->table();

        return QueryPlan::directPrimaryKey(
            $root,
            [$root],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            $token,
            $softDeleteMode,
            $softDeleteMode === null ? null : 'deleted_at',
        );
    }

    private function table(): TableIdentity
    {
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);

        return $table;
    }
}
