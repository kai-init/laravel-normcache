<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Events\MigrationsEnded;
use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\TestCase;

final class EpochInvalidationTest extends TestCase
{
    /**
     * Regression guard: a write must not seed the scope's epoch memo. When it did, the
     * epoch captured before a concurrent flush was reused to stamp published payloads,
     * and the flush's own INCR could land on that same value.
     */
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
