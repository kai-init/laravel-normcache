<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Database\MultipleRecordsFoundException;
use NormCache\Tests\Fixtures\Models\Author;
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
