<?php

namespace NormCache\Tests\Integration\Invalidation;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

/**
 * Behavioral tests: version bumps and model-key evictions are deferred inside database
 * transactions and applied on commit, or discarded on rollback.
 */
class TransactionInvalidationTest extends TestCase
{
    public function test_version_is_not_bumped_mid_transaction(): void
    {
        $versionBefore = NormCache::currentVersion(Author::class);
        $versionDuring = null;

        DB::transaction(function () use (&$versionDuring) {
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
        } catch (\RuntimeException) {
        }

        $this->assertSame($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_model_key_deleted_after_transaction_commits(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        DB::transaction(function () use ($author) {
            $author->update(['name' => 'Alicia']);
        });

        $this->assertNull($this->modelCacheEntry(Author::class, $author->id));
    }

    public function test_transaction_commit_evicts_only_the_updated_model_key(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $alice->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $bob->id));

        DB::transaction(function () use ($alice) {
            $alice->update(['name' => 'Alicia']);
        });

        $this->assertNull($this->modelCacheEntry(Author::class, $alice->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $bob->id));
    }

    public function test_model_key_preserved_after_transaction_rollback(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        try {
            DB::transaction(function () use ($author) {
                $author->update(['name' => 'Alicia']);
                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {
        }

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
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

    public function test_bulk_update_version_is_deferred_inside_transaction(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);
        $versionDuringTx = null;

        DB::transaction(function () use (&$versionDuringTx) {
            Author::where('name', 'Alice')->update(['name' => 'Alicia']);
            $versionDuringTx = NormCache::currentVersion(Author::class);
        });

        $this->assertSame($versionBefore, $versionDuringTx);
    }

    public function test_bulk_update_rollback_does_not_leave_orphaned_version(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $versionBefore = NormCache::currentVersion(Author::class);

        try {
            DB::transaction(function () {
                Author::where('name', 'Alice')->update(['name' => 'Alicia']);
                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {
        }

        $this->assertSame($versionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_bulk_delete_model_key_not_removed_mid_transaction(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));

        $keyExistedMidTx = null;
        $authorId = $author->id;

        try {
            DB::transaction(function () use ($author, $authorId, &$keyExistedMidTx) {
                Author::where('id', $author->id)->delete();
                $keyExistedMidTx = $this->modelCacheEntry(Author::class, $authorId) !== null;
                throw new \RuntimeException('force rollback');
            });
        } catch (\RuntimeException) {
        }

        // Invalidation is deferred to commit; the key must survive both mid-transaction and rollback.
        $this->assertTrue($keyExistedMidTx);
        $this->assertNotNull($this->modelCacheEntry(Author::class, $author->id));
    }

    public function test_committed_transaction_invalidates_outdated_query_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();

        DB::transaction(function () use ($author) {
            $author->update(['name' => 'Alicia']);
        });

        $result = Author::all()->firstWhere('id', $author->id);

        $this->assertSame('Alicia', $result->name);
    }

    public function test_insert_in_transaction_preserves_existing_model_payloads_on_commit(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Author::all();

        $this->assertNotNull($this->modelCacheEntry(Author::class, $alice->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $bob->id));

        DB::transaction(function () {
            Author::create(['name' => 'Carol']);
        });

        // Insert only needs a version bump — existing payloads should not be flushed.
        $this->assertNotNull($this->modelCacheEntry(Author::class, $alice->id));
        $this->assertNotNull($this->modelCacheEntry(Author::class, $bob->id));
    }

    public function test_insert_in_transaction_still_invalidates_query_cache_on_commit(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        DB::transaction(function () {
            Author::create(['name' => 'Bob']);
        });

        $this->assertCount(2, Author::all());
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
        } catch (\RuntimeException) {
        }

        $result = Author::all()->firstWhere('id', $author->id);

        $this->assertSame('Alice', $result->name);
    }

    public function test_single_model_update_in_transaction_bumps_version_once(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::query()->get(); // warm

        $before = NormCache::currentVersion(Author::class);

        DB::transaction(function () use ($author) {
            $author->update(['name' => 'Alicia']);
        });

        $after = NormCache::currentVersion(Author::class);

        $this->assertSame($before + 1, $after);
    }

    public function test_multiple_model_updates_in_transaction_bumps_version_once(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);
        Author::query()->get(); // warm

        $before = NormCache::currentVersion(Author::class);

        DB::transaction(function () use ($a1, $a2) {
            $a1->update(['name' => 'Alicia']);
            $a2->update(['name' => 'Roberto']);
        });

        $after = NormCache::currentVersion(Author::class);

        $this->assertSame($before + 1, $after);
    }

    public function test_flush_and_update_in_transaction_bumps_version_once(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::query()->get(); // warm

        $before = NormCache::currentVersion(Author::class);

        DB::transaction(function () use ($author) {
            NormCache::flushModel(Author::class);
            $author->update(['name' => 'Alicia']);
        });

        $after = NormCache::currentVersion(Author::class);

        $this->assertSame($before + 1, $after);
    }
}
