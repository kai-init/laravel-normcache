<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: scalar and aggregate caching must handle multiple operations on the
 * same base query without key collisions (e.g. sum(views) vs sum(likes)) and correctly
 * bypass the cache for raw expressions.
 */
class ScalarCollisionTest extends TestCase
{
    public function test_sum_collision(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $p1 = Post::create(['title' => 'P1', 'author_id' => $author->id, 'views' => 10]);

        $v1 = Post::sum('views');
        $this->assertEquals(10, $v1);

        $v2 = Post::sum('id');
        $this->assertEquals($p1->id, $v2);
        $this->assertNotEquals(10, $v2);
    }

    public function test_value_collision(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $p1 = Post::create(['title' => 'UniqueTitle', 'author_id' => $author->id, 'views' => 10]);

        $v1 = Post::value('title');
        $this->assertEquals('UniqueTitle', $v1);

        $v2 = Post::value('id');
        $this->assertEquals($p1->id, $v2);
        $this->assertNotEquals('UniqueTitle', $v2);
    }

    public function test_pluck_collision(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $p1 = Post::create(['title' => 'T1', 'author_id' => $author->id, 'views' => 10]);

        $v1 = Post::pluck('title')->toArray();
        $this->assertEquals(['T1'], $v1);

        $v2 = Post::pluck('id')->toArray();
        $this->assertEquals([$p1->id], $v2);
        $this->assertNotEquals(['T1'], $v2);
    }

    public function test_pluck_keyed_collision(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $p1 = Post::create(['title' => 'T1', 'author_id' => $author->id, 'views' => 10]);

        $v1 = Post::pluck('title', 'id')->toArray();
        $this->assertEquals([$p1->id => 'T1'], $v1);

        $v2 = Post::pluck('id', 'title')->toArray();
        $this->assertEquals(['T1' => $p1->id], $v2);
    }

    public function test_raw_expression_scalar_bypasses_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id, 'views' => 10]);

        $sum = Post::sum(DB::raw('views + 1'));
        $this->assertEquals(11, $sum);

        $this->assertEmpty($this->redisKeys('test:scalar:*'));
    }

    public function test_scalar_aggregate_alias_tracks_related_model_dependency(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);

        $this->assertSame(1, Author::withCount('posts')->value('posts_count'));
        $this->assertSame(1, Author::withCount('posts')->value('posts_count'));

        Post::create(['title' => 'P2', 'author_id' => $author->id]);

        $this->assertSame(2, Author::withCount('posts')->value('posts_count'));
    }
}
