<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheStateResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\TestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;
use PHPUnit\Framework\Attributes\DataProvider;

final class CacheStateResolverTest extends TestCase
{
    public function test_a_freshly_resolved_state_is_current(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash');

        $this->assertTrue($resolver->isCurrent($plan, $state));
    }

    public function test_a_state_stops_being_current_after_its_table_is_invalidated(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->cacheStore()->increment($this->cacheKeys()->version($plan->root));

        $this->assertFalse($resolver->isCurrent($plan, $state));
    }

    public function test_a_state_stops_being_current_after_an_epoch_flush_it_has_already_memoized(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->assertTrue($resolver->isCurrent($plan, $state));

        $this->cacheStore()->increment($this->cacheKeys()->epoch());

        $this->assertFalse($resolver->isCurrent($plan, $state));
    }

    public function test_a_dependency_bump_invalidates_a_multi_table_state(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $root = $this->table('posts');
        $dependency = $this->table('authors');
        $plan = QueryPlan::canonical(
            $root,
            [$root, $dependency],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->assertTrue($resolver->isCurrent($plan, $state));

        $this->cacheStore()->increment($this->cacheKeys()->version($dependency));

        $this->assertFalse($resolver->isCurrent($plan, $state));
    }

    #[DataProvider('generationlessPlans')]
    public function test_routes_without_a_generation_survive_a_broad_invalidation(
        string $factory,
    ): void {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->{$factory}();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->assertSame('0', $state->generation);
        $this->assertTrue($resolver->isCurrent($plan, $state));

        $this->cacheStore()->increment($this->cacheKeys()->generation($plan->root));

        $this->assertTrue($resolver->isCurrent($plan, $state));
    }

    public static function generationlessPlans(): array
    {
        return [
            'result' => ['resultPlan'],
            'query group' => ['queryGroupPlan'],
        ];
    }

    public function test_canonical_routes_still_fail_after_a_generation_bump(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->cacheStore()->increment($this->cacheKeys()->generation($plan->root));

        $this->assertFalse($resolver->isCurrent($plan, $state));
    }

    public function test_a_canonical_result_overlay_can_resolve_without_row_generation(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash', usesGeneration: false);
        $this->assertSame('0', $state->generation);

        $this->cacheStore()->increment($this->cacheKeys()->generation($plan->root));

        $this->assertTrue($resolver->isCurrent($plan, $state, usesGeneration: false));
    }

    private function resultPlan(): QueryPlan
    {
        $root = $this->table('posts');

        return QueryPlan::result(
            $root,
            [$root],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );
    }

    private function queryGroupPlan(): QueryPlan
    {
        $root = $this->table('posts');

        return QueryPlan::queryGroup($root, [$root, $this->table('authors')]);
    }

    private function canonicalPlan(): QueryPlan
    {
        $root = $this->table('posts');

        return QueryPlan::canonical(
            $root,
            [$root],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );
    }

    private function table(string $name): TableIdentity
    {
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $name);
        $this->assertNotNull($table);

        return $table;
    }
}
