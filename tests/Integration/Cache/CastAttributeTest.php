<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\Event;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: Eloquent model casts (array, boolean) must round-trip correctly
 * through query-cache, model-cache, and through-relation-cache paths.
 */
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

    public function test_boolean_cast_is_applied_to_cached_pluck_values(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'published' => false]);
        Post::create(['title' => 'B', 'author_id' => $author->id, 'published' => true]);

        $cold = Post::orderBy('id')->pluck('published');
        $warm = Post::orderBy('id')->pluck('published');

        $this->assertSame([false, true], $cold->all());
        $this->assertSame($cold->all(), $warm->all());
    }

    public function test_boolean_cast_is_applied_to_cached_value(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'published' => true]);

        $cold = Post::value('published');
        $warm = Post::value('published');

        $this->assertTrue($cold);
        $this->assertSame($cold, $warm);
    }

    public function test_array_cast_is_applied_to_cached_value_and_pluck(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'metadata' => $this->metadata]);

        $this->assertSame($this->metadata, Post::value('metadata'));
        $this->assertSame([$this->metadata], Post::pluck('metadata')->all());
    }

    public function test_date_attributes_are_applied_to_cached_value_and_pluck(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id]);

        $value = Post::value('created_at');
        $pluck = Post::pluck('created_at')->first();

        $this->assertInstanceOf(\DateTimeInterface::class, $value);
        $this->assertInstanceOf(\DateTimeInterface::class, $pluck);
        $this->assertSame($value->format('Y-m-d H:i:s'), $pluck->format('Y-m-d H:i:s'));
    }

    public function test_pluck_preserves_retrieved_events_for_casted_values(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'A', 'author_id' => $author->id, 'published' => true]);
        $retrieved = 0;

        Event::listen('eloquent.retrieved: ' . Post::class, function () use (&$retrieved): void {
            $retrieved++;
        });

        Post::pluck('published');

        $this->assertSame(1, $retrieved);
    }

    public function test_array_cast_survives_model_cache_db_fallback(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'A', 'author_id' => $author->id, 'metadata' => $this->metadata]);

        Post::all();
        $this->evictModelCache(Post::class, $post->id);

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
