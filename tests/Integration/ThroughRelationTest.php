<?php

namespace NormCache\Tests\Integration;

use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

class ThroughRelationTest extends TestCase
{
    public function test_has_many_through_caches_results(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $first = $country->posts()->get()->pluck('title');

        $keyCountAfterFirst = count($this->redisKeys('test:*'));

        $second = $country->posts()->get()->pluck('title');

        $this->assertEquals($first, $second);
        $this->assertSame($keyCountAfterFirst, count($this->redisKeys('test:*')));
    }

    public function test_has_many_through_returns_correct_results(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);
        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        $titles = $country->posts()->get()->pluck('title')->sort()->values();

        $this->assertSame(['Post A', 'Post B'], $titles->all());
    }

    public function test_has_many_through_cache_invalidated_when_post_version_changes(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'Post A', 'author_id' => $author->id]);

        $country->posts()->get();

        Post::create(['title' => 'Post B', 'author_id' => $author->id]);

        $titles = $country->posts()->get()->pluck('title')->sort()->values();

        $this->assertSame(['Post A', 'Post B'], $titles->all());
    }
}
