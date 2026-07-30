<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheStateResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\TestCase;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class CacheStateResolverTest extends TestCase
{
    public function test_a_freshly_resolved_state_is_current(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        [$state] = $resolver->resolve($plan, 'n', 'hash');

        $this->assertTrue($resolver->isCurrent($plan, $state));
    }

    public function test_a_state_stops_being_current_after_its_table_is_invalidated(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        [$state] = $resolver->resolve($plan, 'n', 'hash');
        $this->cacheStore()->increment($this->cacheKeys()->version($plan->root));

        $this->assertFalse($resolver->isCurrent($plan, $state));
    }

    public function test_a_state_stops_being_current_after_an_epoch_flush_it_has_already_memoized(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        [$state] = $resolver->resolve($plan, 'n', 'hash');
        $this->assertTrue($resolver->isCurrent($plan, $state));

        // The epoch is memoized for this scope by now, so isCurrent() must still
        // re-read it rather than trusting the remembered value.
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
            materializeResult: false,
        );

        [$state] = $resolver->resolve($plan, 'n', 'hash');
        $this->assertTrue($resolver->isCurrent($plan, $state));

        $this->cacheStore()->increment($this->cacheKeys()->version($dependency));

        $this->assertFalse($resolver->isCurrent($plan, $state));
    }

    private function canonicalPlan(): QueryPlan
    {
        $root = $this->table('posts');

        return QueryPlan::canonical(
            $root,
            [$root],
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            materializeResult: false,
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
