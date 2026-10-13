<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Integration\Contract\Concerns\CreatesEloquentContractFixtures;
use NormCache\Tests\TestCase;

/**
 * Contract tests for Eloquent write behavior.
 */
final class EloquentWriteContractTest extends TestCase
{
    use CreatesEloquentContractFixtures;

    public function test_insert_returns_bool(): void
    {
        $native = Author::withoutCache()->insert(['name' => 'Test1', 'created_at' => now(), 'updated_at' => now()]);
        $cached = Author::insert(['name' => 'Test2', 'created_at' => now(), 'updated_at' => now()]);

        $this->assertSame($native, $cached);
        $this->assertIsBool($cached);
    }

    public function test_update_returns_affected_row_count(): void
    {
        $this->fixtures();

        $native = Author::withoutCache()->where('name', 'Alice')->update(['name' => 'Alice1']);
        $cached = Author::where('name', 'Bob')->update(['name' => 'Bob1']);

        $this->assertSame($native, $cached);
        $this->assertIsInt($cached);
    }

    public function test_delete_returns_affected_row_count(): void
    {
        Author::create(['name' => 'Delete1', 'created_at' => now(), 'updated_at' => now()]);
        Author::create(['name' => 'Delete2', 'created_at' => now(), 'updated_at' => now()]);

        $native = Author::withoutCache()->where('name', 'Delete1')->delete();
        $cached = Author::where('name', 'Delete2')->delete();

        $this->assertSame($native, $cached);
        $this->assertIsInt($cached);
    }

    public function test_insert_or_ignore_returns_affected_row_count(): void
    {
        Author::create(['name' => 'Existing']);

        $native = Author::withoutCache()->insertOrIgnore([
            ['name' => 'New1', 'created_at' => now(), 'updated_at' => now()],
        ]);
        $cached = Author::insertOrIgnore([
            ['name' => 'New2', 'created_at' => now(), 'updated_at' => now()],
        ]);

        $this->assertSame($native, $cached);
        $this->assertIsInt($cached);
    }

    public function test_upsert_returns_affected_row_count(): void
    {
        $tag1 = Tag::create(['name' => 'original1']);
        $tag2 = Tag::create(['name' => 'original2']);

        $native = Tag::withoutCache()->upsert(
            [['id' => $tag1->id, 'name' => 'updated1', 'created_at' => now(), 'updated_at' => now()]],
            ['id'], ['name'],
        );
        $cached = Tag::upsert(
            [['id' => $tag2->id, 'name' => 'updated2', 'created_at' => now(), 'updated_at' => now()]],
            ['id'], ['name'],
        );

        $this->assertSame($native, $cached);
    }

    public function test_mutating_primary_key_evicts_old_model_cache_key(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $oldId = $author->id;

        Author::find($oldId);

        $connection = $this->app['db']->connection('testing');
        $table = $this->app->make(TableIdentityResolver::class)->resolve($connection, 'authors');
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $oldRowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $oldId);

        $this->assertNotNull($this->cacheStore()->getRaw($oldRowKey), 'expected the old PK canonical row to be cached');

        $author->id = 9999;
        $author->save();

        // Old row key must be evicted — not left as orphaned memory.
        $this->assertNull($this->cacheStore()->getRaw($oldRowKey), 'old canonical row must be evicted after PK mutation');
    }

    public function test_force_delete_returns_affected_row_count(): void
    {
        $p1 = Post::create(['title' => 'FD1', 'author_id' => Author::create(['name' => 'X'])->id]);
        $p2 = Post::create(['title' => 'FD2', 'author_id' => Author::create(['name' => 'Y'])->id]);
        $p1->delete();
        $p2->delete();

        $native = Post::withoutCache()->onlyTrashed()->where('title', 'FD1')->forceDelete();
        $cached = Post::onlyTrashed()->where('title', 'FD2')->forceDelete();

        $this->assertSame($native, $cached);
    }
}
