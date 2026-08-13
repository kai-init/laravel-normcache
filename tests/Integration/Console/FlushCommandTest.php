<?php

namespace NormCache\Tests\Integration\Console;

use Illuminate\Support\Facades\DB;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class FlushCommandTest extends TestCase
{
    public function test_advances_the_epoch_and_orphans_every_warm_entry(): void
    {
        Author::create(['name' => 'Alice']);

        $query = static fn() => Author::orderBy('id')->get();
        $query();
        $query();

        $before = $this->cacheStore()->getRaw($this->cacheKeys()->epoch());

        $this->artisan('normcache:flush')
            ->expectsOutputToContain('NormCache global epoch advanced.')
            ->assertSuccessful();

        $this->assertNotSame(
            $before,
            $this->cacheStore()->getRaw($this->cacheKeys()->epoch()),
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $query();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotSame([], $queries, 'a flushed cache must fall back to the database');
    }

    public function test_reports_failure_when_the_epoch_cannot_be_advanced(): void
    {
        $this->app->instance(RedisStore::class, new RedisStore('missing-normcache-connection'));
        $this->app->forgetScopedInstances();

        $this->artisan('normcache:flush')
            ->expectsOutputToContain('NormCache global invalidation failed.')
            ->assertFailed();
    }
}
