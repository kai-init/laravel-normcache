<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\CanonicalRowRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\TestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class CanonicalRowRepositoryTest extends TestCase
{
    public function test_a_missing_row_reports_no_epoch_and_no_reason(): void
    {
        $cached = $this->app->make(CanonicalRowRepository::class)->read($this->plan('i:404'));

        $this->assertNull($cached->row);
        $this->assertNull($cached->epoch);
        $this->assertNull($cached->reason);
    }

    public function test_a_corrupt_payload_is_reported_as_such(): void
    {
        $plan = $this->plan('i:1');
        $repository = $this->app->make(CanonicalRowRepository::class);
        $generation = $repository->read($plan)->generation;

        $this->cacheStore()->setRawForever(
            $this->cacheKeys()->row($plan->root, $generation, 'i:1'),
            'not-a-payload',
        );

        $this->assertSame('corrupt_payload', $repository->read($plan)->reason);
    }

    public function test_soft_delete_visibility_filters_by_plan_mode(): void
    {
        $repository = $this->app->make(CanonicalRowRepository::class);
        $live = (object) ['id' => 1, 'deleted_at' => null];
        $trashed = (object) ['id' => 2, 'deleted_at' => '2026-01-01 00:00:00'];

        $this->assertSame([$live], $repository->visibleRows($this->plan('i:1', 'default'), $live));
        $this->assertSame([], $repository->visibleRows($this->plan('i:2', 'default'), $trashed));
        $this->assertSame([$trashed], $repository->visibleRows($this->plan('i:2', 'only'), $trashed));
        $this->assertSame([], $repository->visibleRows($this->plan('i:1', 'only'), $live));
        $this->assertSame([$trashed], $repository->visibleRows($this->plan('i:2', 'with'), $trashed));
    }

    public function test_visibility_reports_a_row_missing_its_deleted_at_column(): void
    {
        $repository = $this->app->make(CanonicalRowRepository::class);

        $this->assertNull(
            $repository->visibleRows($this->plan('i:1', 'default'), (object) ['id' => 1]),
        );
    }

    public function test_a_plan_without_soft_deletes_passes_every_row_through(): void
    {
        $row = (object) ['id' => 1];

        $this->assertSame(
            [$row],
            $this->app->make(CanonicalRowRepository::class)->visibleRows($this->plan('i:1'), $row),
        );
    }

    public function test_row_state_is_scoped_to_the_generation_and_token(): void
    {
        $plan = $this->plan('i:7');
        $state = $this->app->make(CanonicalRowRepository::class)->state($plan, '3', '9');

        $this->assertSame($this->cacheKeys()->row($plan->root, '3', 'i:7'), $state->key);
        $this->assertSame('3', $state->generation);
        $this->assertSame('9', $state->epoch);
        $this->assertSame('0', $state->version);
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
