<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class PublicInvalidationTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Public',
            'views' => 1,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_flush_all_is_one_epoch_increment_and_invalidates_every_payload(): void
    {
        $read = fn() => DB::table('posts')->where('id', $this->postId)->first();
        $read();
        $read();

        $before = (int) ($this->cacheStore()->getRaw(
            $this->cacheKeys()->epoch(),
        ) ?? '0');

        $this->assertTrue(NormCache::flushAll());
        $this->assertSame(
            $before + 1,
            (int) $this->cacheStore()->getRaw($this->cacheKeys()->epoch()),
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_flush_tag_invalidates_only_that_query_namespace(): void
    {
        $tagged = fn() => DB::table('posts')->where('id', $this->postId)->tag('homepage')->get();
        $untagged = fn() => DB::table('posts')->where('id', $this->postId)->get();

        $tagged();
        $untagged();
        $tagged();
        $untagged();

        $this->assertTrue(NormCache::flushTag('homepage'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $tagged();
        $untagged();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_table_invalidation_is_broad_and_boolean(): void
    {
        DB::table('posts')->get();
        $this->assertTrue(NormCache::invalidateTable('testing', 'posts'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        DB::table('posts')->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }
}
