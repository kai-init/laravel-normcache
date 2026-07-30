<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Events\MigrationsEnded;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use Psr\Log\LoggerInterface;

final class CacheUnavailableTest extends TestCase
{
    public function test_first_read_fails_open_when_redis_is_not_configured(): void
    {
        DB::connection()->getPdo()->exec(
            "insert into authors (id, name) values (1, 'Author')"
        );
        DB::connection()->getPdo()->exec(
            "insert into posts
                (id, title, views, published, author_id, created_at, updated_at)
             values
                (1, 'Live database', 0, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        );
        $this->useMissingRedisConnection();

        $first = DB::table('posts')->where('id', 1)->first();
        $second = DB::table('posts')->where('id', 1)->first();

        $this->assertSame('Live database', $first?->title);
        $this->assertSame('Live database', $second?->title);
    }

    public function test_cache_disabled_status_fails_open_when_redis_is_unavailable(): void
    {
        $store = $this->app->make(RedisStore::class);

        try {
            $this->app->instance(
                RedisStore::class,
                new RedisStore('missing-normcache-connection'),
            );
            $this->app->forgetScopedInstances();

            $this->assertFalse($this->cacheManager()->cacheDisabled());
            $this->assertFalse($this->app->make(CacheRuntime::class)->available());
        } finally {
            $this->app->instance(RedisStore::class, $store);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_reads_stop_entering_the_cache_path_after_a_failure(): void
    {
        $store = $this->app->make(RedisStore::class);

        try {
            $this->app->instance(
                RedisStore::class,
                new RedisStore('missing-normcache-connection'),
            );
            $this->app->forgetScopedInstances();

            $runtime = $this->app->make(CacheRuntime::class);

            $this->assertFalse($runtime->readable());
            $this->assertFalse($this->app->make(CacheRuntime::class)->available());
            $this->assertFalse(
                $runtime->readable(),
                'a scope that has already failed must not re-enter the cache path',
            );
        } finally {
            $this->app->instance(RedisStore::class, $store);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_completed_migrations_do_not_fail_when_redis_is_unavailable(): void
    {
        $store = $this->app->make(RedisStore::class);

        try {
            $this->app->instance(
                RedisStore::class,
                new RedisStore('missing-normcache-connection'),
            );
            $this->app->forgetScopedInstances();

            $this->app['events']->dispatch(new MigrationsEnded('up'));

            $this->assertFalse($this->app->make(CacheRuntime::class)->available());
        } finally {
            $this->app->instance(RedisStore::class, $store);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_first_write_fails_open_when_redis_is_not_configured(): void
    {
        $this->useMissingRedisConnection();

        DB::table('authors')->insert(['name' => 'Still written']);
        DB::table('authors')->where('name', 'Still written')->update([
            'name' => 'Updated',
        ]);

        $this->assertTrue(
            DB::table('authors')->where('name', 'Updated')->exists(),
        );
    }

    public function test_failed_invalidation_is_logged_as_critical(): void
    {
        $authorId = DB::table('authors')->insertGetId(['name' => 'Author']);
        DB::table('posts')->insert([
            'title' => 'Before',
            'author_id' => $authorId,
        ]);

        $store = $this->app->make(RedisStore::class);
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('critical');

        try {
            $this->app->instance(LoggerInterface::class, $logger);
            $this->app->instance(
                RedisStore::class,
                new RedisStore('missing-normcache-connection'),
            );
            $this->app->forgetScopedInstances();

            DB::table('posts')->where('id', 1)->update(['title' => 'After']);
        } finally {
            $this->app->instance(RedisStore::class, $store);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_disabled_cache_reads_do_not_suppress_write_invalidation(): void
    {
        DB::table('authors')->insert(['id' => 1, 'name' => 'Author']);
        DB::table('posts')->insert([
            'id' => 1,
            'title' => 'Before',
            'views' => 0,
            'published' => true,
            'author_id' => 1,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
        DB::table('posts')->where('id', 1)->first();

        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);
        $versionKey = $this->cacheKeys()->version($table);
        $before = $this->cacheStore()->getRaw($versionKey) ?? '0';

        $this->app->make(CacheRuntime::class)->disable();
        DB::table('posts')->where('id', 1)->update(['title' => 'After']);

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($versionKey));
        $this->app->forgetScopedInstances();
        $this->assertSame('After', DB::table('posts')->where('id', 1)->first()?->title);
    }

    private function useMissingRedisConnection(): void
    {
        $config = (array) config('normcache');
        $config['connection'] = 'missing-normcache-connection';
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
    }
}
