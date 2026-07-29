<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
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

    public function test_invalidate_accepts_table_names_models_and_model_classes(): void
    {
        Post::query()->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);
        $versionKey = $this->cacheKeys()->version($identity);
        $generationKey = $this->cacheKeys()->generation($identity);
        $version = (int) ($this->cacheStore()->getRaw($versionKey) ?? '0');
        $generation = (int) ($this->cacheStore()->getRaw($generationKey) ?? '0');

        $this->assertTrue(NormCache::invalidate([
            'posts',
            Post::class,
            new Post,
        ]));
        $this->assertSame($version + 1, (int) $this->cacheStore()->getRaw($versionKey));
        $this->assertSame($generation + 1, (int) $this->cacheStore()->getRaw($generationKey));

        DB::flushQueryLog();
        DB::enableQueryLog();
        Post::query()->get();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }
}
