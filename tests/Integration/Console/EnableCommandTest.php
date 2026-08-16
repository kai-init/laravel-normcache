<?php

namespace NormCache\Tests\Integration\Console;

use NormCache\Facades\NormCache;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class EnableCommandTest extends TestCase
{
    public function test_reports_the_new_epoch_and_clears_the_disabled_flag(): void
    {
        $before = (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');

        $this->artisan('normcache:disable')->assertSuccessful();
        $this->app->forgetScopedInstances();

        $this->artisan('normcache:enable')
            ->expectsOutputToContain('epoch ' . ($before + 1))
            ->assertSuccessful();

        $this->assertFalse(NormCache::cacheDisabled());
        $this->assertNull($this->cacheStore()->getRaw($this->cacheKeys()->disabled()));
    }

    public function test_refuses_when_the_cache_is_off_by_configuration(): void
    {
        $config = (array) config('normcache');
        $config['enabled'] = false;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        $this->artisan('normcache:enable')
            ->expectsOutputToContain('NORMCACHE_ENABLED=true')
            ->assertFailed();
    }
}
