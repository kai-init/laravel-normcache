<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Concerns\MakesTestModels;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

/**
 * Tag namespacing and flushTag validation.
 *
 * Contract test: the native path (withoutCache), the cold-cache path
 * (miss -> DB) and the warm-cache path (hit) must all return the same thing.
 * A failure means NormCache's cached result diverges from native Eloquent.
 */
final class TagsTest extends TestCase
{
    use MakesTestModels;

    public function test_flush_tag_clears_aggregate_cache_for_tagged_query(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        Author::query()->tag('home')->withCount('posts')->get();

        DB::table('posts')->insert(['title' => 'P2', 'author_id' => $alice->id, 'created_at' => now(), 'updated_at' => now()]);

        $this->cacheManager()->flushTag('home');

        $result = Author::query()->tag('home')->withCount('posts')->get();

        $this->assertSame(2, $result->first()->posts_count, 'flushTag must clear tagged aggregate cache entries');
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
