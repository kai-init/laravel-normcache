<?php

namespace NormCache\Tests\Integration\Console;

use NormCache\Facades\NormCache;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class DisableCommandTest extends TestCase
{
    public function test_disables_the_cache_for_every_node(): void
    {
        $this->artisan('normcache:disable')
            ->expectsOutputToContain('NormCache disabled.')
            ->assertSuccessful();

        // Verify through a scope without the command's memoized state.
        $this->app->forgetScopedInstances();

        $this->assertTrue(NormCache::cacheDisabled());
    }

    public function test_is_a_no_op_when_the_cache_is_already_off_by_configuration(): void
    {
        $config = (array) config('normcache');
        $config['enabled'] = false;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        $this->artisan('normcache:disable')
            ->expectsOutputToContain('already disabled by configuration')
            ->assertSuccessful();

        $this->assertNull($this->cacheStore()->getRaw($this->cacheKeys()->disabled()));
    }
}
