<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;

final class RuntimeRefreshTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('normcache.epoch_refresh_seconds', 1);
    }

    protected function setUp(): void
    {
        parent::setUp();

        Author::query()->toBase()->insert(['id' => 1, 'name' => 'Author']);
        RawPost::query()->toBase()->insert([
            'id' => 1,
            'title' => 'Before',
            'views' => 0,
            'published' => true,
            'author_id' => 1,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_external_disable_reaches_a_live_scope_after_the_refresh_interval(): void
    {
        $read = fn() => RawPost::query()->toBase()->where('id', 1)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        // Simulates another process disabling the cache without touching this scope.
        $this->cacheStore()->setRawForever($this->cacheKeys()->disabled(), '1');
        DB::connection()->getPdo()->exec("update posts set title = 'Changed' where id = 1");

        usleep(1_050_000);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $read();
        DB::disableQueryLog();

        $this->assertSame('Changed', $actual);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_external_enable_reaches_a_disabled_live_scope_after_the_refresh_interval(): void
    {
        $disabledKey = $this->cacheKeys()->disabled();
        $epochKey = $this->cacheKeys()->epoch();
        $read = fn() => RawPost::query()->toBase()->where('id', 1)->value('title');

        $this->cacheStore()->setRawForever($disabledKey, '1');

        // Establish a live scope that has memoized the disabled state.
        $this->assertSame('Before', $read());

        $this->cacheStore()->enableCache($epochKey, $disabledKey);
        usleep(1_050_000);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_external_disable_stops_live_scope_invalidation_after_the_refresh_interval(): void
    {
        RawPost::query()->toBase()->where('id', 1)->first();

        $versionKey = $this->cacheKeys()->version($this->postsTable());
        $before = $this->cacheStore()->getRaw($versionKey) ?? '0';

        // Simulates another process disabling the cache after this scope memoized "enabled".
        $this->cacheStore()->setRawForever($this->cacheKeys()->disabled(), '1');
        usleep(1_050_000);

        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'After']);

        $this->assertSame($before, $this->cacheStore()->getRaw($versionKey) ?? '0');
    }

    private function postsTable()
    {
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);

        return $table;
    }
}
