<?php

namespace NormCache\Tests\Integration;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class CastAttributeTest extends TestCase
{
    private array $metadata = ['section' => 'tech', 'tags' => ['php', 'redis'], 'views' => 0];

    public function test_array_cast_survives_query_cache_round_trip(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'metadata' => $this->metadata]);

        Post::all();
        $post = Post::all()->first();

        $this->assertIsArray($post->metadata);
        $this->assertSame($this->metadata, $post->metadata);
    }

    public function test_boolean_cast_survives_query_cache_round_trip(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'published' => false]);

        Post::all();
        $post = Post::all()->first();

        $this->assertIsBool($post->published);
        $this->assertFalse($post->published);
    }

    public function test_array_cast_survives_model_cache_db_fallback(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'A', 'author_id' => $author->id, 'metadata' => $this->metadata]);

        Post::all();
        NormCache::delete(NormCache::modelKey(Post::class, $post->id));

        $fromFallback = Post::all()->first();

        $this->assertIsArray($fromFallback->metadata);
        $this->assertSame($this->metadata, $fromFallback->metadata);
    }

    public function test_array_cast_survives_through_relation_cache(): void
    {
        $country = Country::create(['name' => 'AU']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'metadata' => $this->metadata]);

        $country->posts()->get();
        $post = $country->posts()->get()->first();

        $this->assertIsArray($post->metadata);
        $this->assertSame($this->metadata, $post->metadata);
    }
}
