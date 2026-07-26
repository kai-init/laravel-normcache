<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

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

    private function useMissingRedisConnection(): void
    {
        $config = (array) config('normcache');
        $config['connection'] = 'missing-normcache-connection';
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
    }
}
