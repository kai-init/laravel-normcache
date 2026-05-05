<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class TransactionInvalidationTest extends TestCase
{
    public function test_version_is_not_bumped_mid_transaction(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);
        $versionDuring = null;

        DB::transaction(function () use ($versionBefore, &$versionDuring) {
            Author::create(['name' => 'Alice']);
            $versionDuring = NormCache::currentVersion(Author::class);
        });

        $this->assertSame($versionBefore, $versionDuring);
        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_committed_transaction_bumps_version(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);

        DB::transaction(function () {
            Author::create(['name' => 'Alice']);
        });

        $this->assertGreaterThan($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_rolled_back_transaction_does_not_bump_version(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);

        try {
            DB::transaction(function () {
                Author::create(['name' => 'Alice']);
                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {}

        $this->assertSame($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_model_key_deleted_after_transaction_commits(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        DB::transaction(function () use ($author) {
            $author->update(['name' => 'Alicia']);
        });

        $this->assertNull(NormCache::get($modelKey));
    }

    public function test_model_key_preserved_after_transaction_rollback(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $modelKey = NormCache::modelKey(Author::class, $author->id);
        $this->assertNotNull(NormCache::get($modelKey));

        try {
            DB::transaction(function () use ($author) {
                $author->update(['name' => 'Alicia']);
                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {}

        $this->assertNotNull(NormCache::get($modelKey));
    }

    public function test_multiple_writes_to_same_model_in_transaction_produce_one_version_bump(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);

        DB::transaction(function () {
            Author::create(['name' => 'Alice']);
            Author::create(['name' => 'Bob']);
            Author::create(['name' => 'Carol']);
        });

        $this->assertSame($versionBefore + 1, NormCache::currentVersion(Author::class));
    }

    public function test_committed_transaction_invalidates_stale_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        DB::transaction(function () use ($author) {
            $author->update(['name' => 'Alicia']);
        });

        $result = Author::all()->firstWhere('id', $author->id);

        $this->assertSame('Alicia', $result->name);
    }

    public function test_rolled_back_transaction_leaves_cache_consistent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        try {
            DB::transaction(function () use ($author) {
                $author->update(['name' => 'Alicia']);
                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {}

        $result = Author::all()->firstWhere('id', $author->id);

        $this->assertSame('Alice', $result->name);
    }
}
