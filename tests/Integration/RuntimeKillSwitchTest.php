<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\TestCase;

final class RuntimeKillSwitchTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

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
    }

    public function test_disabled_reads_bypass_to_the_database(): void
    {
        $read = fn() => DB::table('posts')->where('id', 1)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        $this->assertTrue(NormCache::disableCache());
        $this->newScope();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());
        DB::disableQueryLog();

        $this->assertCount(2, DB::getQueryLog());
    }

    public function test_disabled_writes_do_not_invalidate(): void
    {
        DB::table('posts')->where('id', 1)->first();
        $versionKey = $this->cacheKeys()->version($this->postsTable());
        $before = $this->cacheStore()->getRaw($versionKey) ?? '0';

        $this->assertTrue(NormCache::disableCache());
        $this->newScope();

        DB::table('posts')->where('id', 1)->update(['title' => 'After']);

        $this->assertSame($before, $this->cacheStore()->getRaw($versionKey) ?? '0');
    }

    public function test_enable_advances_the_epoch_and_clears_the_flag(): void
    {
        $epochKey = $this->cacheKeys()->epoch();
        DB::table('posts')->where('id', 1)->first();
        $before = (int) ($this->cacheStore()->getRaw($epochKey) ?? '0');

        $this->assertTrue(NormCache::disableCache());
        $this->newScope();
        $this->assertTrue(NormCache::cacheDisabled());

        $epoch = NormCache::enableCache();

        $this->assertSame($before + 1, $epoch);
        $this->assertSame((string) ($before + 1), $this->cacheStore()->getRaw($epochKey));
        $this->assertFalse(NormCache::cacheDisabled());
    }

    public function test_store_enable_returns_the_advanced_epoch_and_clears_the_flag(): void
    {
        $epochKey = $this->cacheKeys()->epoch();
        $disabledKey = $this->cacheKeys()->disabled();
        $this->assertTrue(NormCache::disableCache());
        $before = (int) ($this->cacheStore()->getRaw($epochKey) ?? '0');
        $this->assertNotNull($this->cacheStore()->getRaw($disabledKey));

        $epoch = $this->cacheStore()->enableCache($epochKey, $disabledKey);

        $this->assertSame($before + 1, $epoch);
        $this->assertSame((string) ($before + 1), $this->cacheStore()->getRaw($epochKey));
        $this->assertNull($this->cacheStore()->getRaw($disabledKey));
    }

    public function test_payloads_cached_before_a_disable_are_not_served_after_enable(): void
    {
        $read = fn() => DB::table('posts')->where('id', 1)->value('title');
        $this->assertSame('Before', $read());

        $this->assertTrue(NormCache::disableCache());
        $this->newScope();

        // Written while disabled, so nothing invalidated it.
        DB::table('posts')->where('id', 1)->update(['title' => 'Written while off']);

        $this->assertNotNull(NormCache::enableCache());
        $this->newScope();

        $this->assertSame('Written while off', $read());
    }

    public function test_reads_resume_from_cache_after_enable(): void
    {
        $read = fn() => DB::table('posts')->where('id', 1)->value('title');

        $this->assertTrue(NormCache::disableCache());
        $this->newScope();
        $this->assertNotNull(NormCache::enableCache());
        $this->newScope();

        $this->assertSame('Before', $read());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Before', $read());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_a_flag_set_by_another_process_is_observed_next_scope(): void
    {
        $read = fn() => DB::table('posts')->where('id', 1)->value('title');
        $this->assertSame('Before', $read());

        // Simulates another node's normcache:disable, bypassing this scope's memo.
        $this->cacheStore()->setRawForever($this->cacheKeys()->disabled(), '1');
        $this->newScope();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Before', $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_disable_command_reports_success(): void
    {
        $this->assertSame(0, Artisan::call('normcache:disable'));
        $this->newScope();
        $this->assertTrue(NormCache::cacheDisabled());
    }

    public function test_enable_command_reports_the_new_epoch(): void
    {
        $before = (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');

        $this->assertSame(0, Artisan::call('normcache:disable'));
        $this->newScope();
        $this->assertSame(0, Artisan::call('normcache:enable'));

        $this->assertStringContainsString(
            'epoch ' . ($before + 1),
            Artisan::output(),
        );
        $this->assertFalse(NormCache::cacheDisabled());
    }

    private function postsTable()
    {
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);

        return $table;
    }

    /** The switch is memoized per scope, so a flip is observed by the next request or job. */
    private function newScope(): void
    {
        $this->app->forgetScopedInstances();
    }
}
