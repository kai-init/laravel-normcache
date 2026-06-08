<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Database\MultipleRecordsFoundException;
use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: streaming operations (chunk) and
 * sole() must bypass the query cache entirely.
 */
class StreamingOperationsTest extends TestCase
{
    // Streaming methods — must bypass the cache entirely

    public function test_chunk_does_not_write_query_cache_keys(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        Author::orderBy('id')->chunk(1, fn() => null);

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_chunk_sees_fresh_data_after_version_bump(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all(); // warm
        Author::create(['name' => 'Bob']);

        $names = [];
        Author::orderBy('name')->chunk(10, function ($batch) use (&$names) {
            $names = array_merge($names, $batch->pluck('name')->all());
        });

        $this->assertContains('Bob', $names);
    }

    public function test_temporary_streaming_bypass_does_not_persist_wrapped_eager_load_constraints(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        $query = Author::query()
            ->with('posts')
            ->orderBy('id');

        $query->chunk(1, fn() => null);
        $query->getQuery()->limit = null;
        $query->getQuery()->offset = null;
        $query->get();

        $queries = [];
        DB::listen(function ($query) use (&$queries): void {
            $queries[] = $query->sql;
        });

        $warm = $query->get();

        $this->assertCount(1, $warm);
        $this->assertTrue($warm->first()->relationLoaded('posts'));
        $this->assertSame('Hello', $warm->first()->posts->first()->title);

        $this->assertEmpty($queries, 'Cache should be hit, but these DB queries were executed: ' . implode(', ', $queries));
    }

    // sole()

    public function test_sole_throws_when_row_is_deleted_after_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::where('name', 'Alice')->sole();
        $author->delete();

        $this->expectException(ModelNotFoundException::class);
        Author::where('name', 'Alice')->sole();
    }

    public function test_sole_throws_when_second_row_is_inserted_after_warm_hit(): void
    {
        Author::create(['name' => 'Alice']);
        Author::where('name', 'Alice')->sole();
        Author::create(['name' => 'Alice']);

        $this->expectException(MultipleRecordsFoundException::class);
        Author::where('name', 'Alice')->sole();
    }

    public function test_sole_does_not_populate_the_query_cache(): void
    {
        Author::create(['name' => 'Alice']);
        Author::where('name', 'Alice')->sole();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }
}
