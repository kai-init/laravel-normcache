<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Events\MigrationsEnded;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\CacheManager;
use NormCache\Facades\NormCache;
use NormCache\Invalidator;
use NormCache\Planning\SchemaCatalog;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class EpochInvalidationTest extends TestCase
{
    public function test_flush_all_evicts_a_pk_value_read(): void
    {
        DB::table('authors')->insert(['id' => 1, 'name' => 'Author']);
        DB::table('posts')->insert([
            'id' => 1, 'title' => 'Before', 'views' => 0, 'published' => true,
            'author_id' => 1, 'created_at' => now(), 'updated_at' => now(),
        ]);

        $read = fn() => DB::table('posts')->where('id', 1)->value('title');
        $this->assertSame('Before', $read());

        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");

        $this->assertTrue(NormCache::flushAll());
        $this->app->forgetScopedInstances();

        $this->assertSame('Changed', $read());
    }

    public function test_a_flush_all_from_another_process_reaches_a_live_worker(): void
    {
        DB::table('authors')->insert(['id' => 1, 'name' => 'Author']);
        DB::table('posts')->insert([
            'id' => 1, 'title' => 'Before', 'views' => 0, 'published' => true,
            'author_id' => 1, 'created_at' => now(), 'updated_at' => now(),
        ]);

        $original = $this->app->make(CacheConfig::class);
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray(
            [...(array) config('normcache'), 'epoch_refresh_seconds' => 1],
        ));
        $this->app->forgetScopedInstances();

        $read = fn() => DB::table('posts')->where('id', 1)->value('title');
        $this->assertSame('Before', $read());

        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");
        $this->assertTrue($this->foreignCacheManager()->flushAll());

        usleep(1_050_000);

        try {
            $this->assertSame(
                'Changed',
                $read(),
                'a global flush must reach a live worker once the refresh interval lapses',
            );
        } finally {
            $this->app->instance(CacheConfig::class, $original);
            $this->app->forgetScopedInstances();
        }
    }

    private function foreignCacheManager(): CacheManager
    {
        $config = $this->app->make(CacheConfig::class);
        $store = $this->app->make(RedisStore::class);
        $keys = $this->app->make(CacheKeyBuilder::class);

        return new CacheManager(
            $config,
            new CacheRuntime($config, $store, $keys, $this->app->make(FailureReporter::class)),
            $store,
            $keys,
            $this->app->make(Invalidator::class),
            $this->app->make(SchemaCatalog::class),
            $this->app->make(QueryIdentity::class),
            $this->app->make(FailureReporter::class),
        );
    }

    public function test_an_epoch_advanced_by_another_process_is_observed_next_scope(): void
    {
        DB::table('authors')->insert(['id' => 1, 'name' => 'Author']);
        DB::table('posts')->insert([
            'id' => 1, 'title' => 'Before', 'views' => 0, 'published' => true,
            'author_id' => 1, 'created_at' => now(), 'updated_at' => now(),
        ]);

        $read = fn() => DB::table('posts')->where('id', 1)->value('title');
        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");
        $this->cacheStore()->increment($this->cacheKeys()->epoch());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Before', $read());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());

        $this->app->forgetScopedInstances();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Changed', $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_completed_migrations_advance_the_epoch_while_cache_is_disabled_by_configuration(): void
    {
        $epochKey = $this->cacheKeys()->epoch();
        $before = (int) ($this->cacheStore()->getRaw($epochKey) ?? '0');
        $original = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['enabled'] = false;

        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        try {
            $this->app['events']->dispatch(new MigrationsEnded('up'));
        } finally {
            $this->app->instance(CacheConfig::class, $original);
            $this->app->forgetScopedInstances();
        }

        $this->assertSame(
            $before + 1,
            (int) $this->cacheStore()->getRaw($epochKey),
        );
    }

    public function test_completed_migrations_advance_the_epoch(): void
    {
        DB::table('authors')->insert(['id' => 1, 'name' => 'Author']);
        DB::table('posts')->insert([
            'id' => 1, 'title' => 'Before', 'views' => 0, 'published' => true,
            'author_id' => 1, 'created_at' => now(), 'updated_at' => now(),
        ]);

        $read = fn() => DB::table('posts')->where('id', 1)->value('title');
        $this->assertSame('Before', $read());

        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");
        $this->app['events']->dispatch(new MigrationsEnded('up'));
        $this->app->forgetScopedInstances();

        $this->assertSame('Changed', $read());
    }
}
