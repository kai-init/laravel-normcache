<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Events\MigrationsEnded;
use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
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

        // Change the row behind NormCache's back so nothing invalidates.
        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");

        $this->assertTrue(NormCache::flushAll());
        $this->app->forgetScopedInstances();

        $this->assertSame('Changed', $read());
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
