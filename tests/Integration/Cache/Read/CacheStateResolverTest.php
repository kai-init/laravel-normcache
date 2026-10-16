<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
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

        $this->assertTrue($state->equals($resolver->resolve($plan, 'n', 'hash')));
    }

    public function test_a_state_stops_being_current_after_its_table_is_invalidated(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->cacheStore()->increment($this->cacheKeys()->version($plan->root));

        $this->assertFalse($state->equals($resolver->resolve($plan, 'n', 'hash')));
    }

    public function test_the_next_scope_resolves_an_external_epoch_flush(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->assertTrue($state->equals($resolver->resolve($plan, 'n', 'hash')));

        $this->cacheStore()->increment($this->cacheKeys()->epoch());
        $this->app->forgetScopedInstances();
        $resolver = $this->app->make(CacheStateResolver::class);

        $this->assertFalse($state->equals($resolver->resolve($plan, 'n', 'hash')));
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
        $this->assertTrue($state->equals($resolver->resolve($plan, 'n', 'hash')));

        $this->cacheStore()->increment($this->cacheKeys()->version($dependency));

        $this->assertFalse($state->equals($resolver->resolve($plan, 'n', 'hash')));
    }

    #[DataProvider('generationlessPlans')]
    public function test_routes_without_a_generation_survive_a_broad_invalidation(
        string $factory,
    ): void {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->{$factory}();

        $state = $resolver->resolve($plan, 'n', 'hash');
        $this->assertSame('0', $state->generation);
        $this->assertTrue($state->equals($resolver->resolve($plan, 'n', 'hash')));

        $this->cacheStore()->increment($this->cacheKeys()->generation($plan->root));

        $this->assertTrue($state->equals($resolver->resolve($plan, 'n', 'hash')));
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

        $this->assertFalse($state->equals($resolver->resolve($plan, 'n', 'hash')));
    }

    public function test_a_canonical_result_overlay_can_resolve_without_row_generation(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $plan = $this->canonicalPlan();

        $state = $resolver->resolve($plan, 'n', 'hash', usesGeneration: false);
        $this->assertSame('0', $state->generation);

        $this->cacheStore()->increment($this->cacheKeys()->generation($plan->root));

        $this->assertTrue($state->equals($resolver->resolve($plan, 'n', 'hash', usesGeneration: false)));
    }

    #[DataProvider('warmReadPlans')]
    public function test_known_version_warm_reads_match_the_generic_state(
        string $factory,
        ?string $knownGeneration,
        ?bool $usesGeneration,
    ): void {
        $resolver = $this->app->make(CacheStateResolver::class);
        $this->app->make(CacheRuntime::class)->epoch();
        $plan = $this->{$factory}();

        $warm = $resolver->resolve(
            $plan,
            'n',
            'hash',
            knownVersion: '4',
            knownGeneration: $knownGeneration,
            usesGeneration: $usesGeneration,
        );
        $generic = $resolver->resolve(
            $plan,
            'n',
            'hash',
            knownVersion: '4',
            knownGeneration: $knownGeneration,
            usesGeneration: $usesGeneration,
            prefetched: [],
        );

        $this->assertTrue($warm->equals($generic));
    }

    /** @return array<string, array{string, ?string, ?bool}> */
    public static function warmReadPlans(): array
    {
        return [
            'result explicit generation disabled' => ['resultPlan', null, false],
            'result default generation setting' => ['resultPlan', null, null],
            'canonical overlay' => ['canonicalPlan', null, false],
            'canonical membership generation' => ['canonicalPlan', '7', null],
        ];
    }

    public function test_known_version_warm_reads_keep_tag_and_dependency_keys(): void
    {
        $resolver = $this->app->make(CacheStateResolver::class);
        $this->app->make(CacheRuntime::class)->epoch();
        $root = $this->table('posts');
        $dependency = $this->table('authors');
        $multiTable = QueryPlan::result($root, [$root, $dependency]);

        $tagged = $resolver->resolve(
            $this->resultPlan(),
            'g' . str_repeat('a', 32),
            'hash',
            knownVersion: '0',
            usesGeneration: false,
        );
        $dependent = $resolver->resolve(
            $multiTable,
            'n',
            'hash',
            knownVersion: '0',
            usesGeneration: false,
        );

        $this->assertNotNull($tagged->tagKey);
        $this->assertSame([$dependency->hash => '0'], $dependent->versions);
        $this->assertContains($this->cacheKeys()->version($dependency), $resolver->pendingStateKeys($multiTable, 'n'));
    }

    private function resultPlan(): QueryPlan
    {
        $root = $this->table('posts');

        return QueryPlan::result(
            $root,
            [$root],
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
