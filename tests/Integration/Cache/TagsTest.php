<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class TagsTest extends TestCase
{
    public function test_flush_tag_clears_aggregate_cache_for_tagged_query(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);
        $query = fn() => Author::query()->tag('home')->withCount('posts')->get();

        $query();
        $this->assertWarmCacheHit($query);

        DB::statement(
            'insert into posts (title, author_id, created_at, updated_at) values (?, ?, ?, ?)',
            ['P2', $alice->id, now(), now()],
        );

        $stale = null;
        $this->assertWarmCacheHit(function () use ($query, &$stale) {
            return $stale = $query();
        });
        $this->assertSame(1, $stale->first()->posts_count);

        $this->cacheManager()->flushTag('home');

        $fresh = null;
        $this->assertColdCacheMiss(function () use ($query, &$fresh) {
            return $fresh = $query();
        });
        $this->assertSame(2, $fresh->first()->posts_count, 'flushTag must clear tagged aggregate cache entries');
        $this->assertWarmCacheHit($query);
    }

    public function test_flush_tag_allows_arbitrary_characters(): void
    {
        // Tags are hashed before use in any Redis key, so there is
        // no character-safety restriction — only emptiness/UTF-8/length are validated.
        $this->assertTrue($this->cacheManager()->flushTag('tag:with:colons/and*stars'));
    }

    public function test_flush_tag_rejects_empty_tag(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->cacheManager()->flushTag('');
    }

    public function test_flush_tag_rejects_invalid_utf8(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->cacheManager()->flushTag("\xB1\x31");
    }

    public function test_flush_tag_rejects_tag_over_128_bytes(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->cacheManager()->flushTag(str_repeat('a', 129));
    }
}
