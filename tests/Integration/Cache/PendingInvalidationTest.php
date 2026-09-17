<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Invalidator;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class PendingInvalidationTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('normcache.max_precise_invalidation_keys', 3);
    }

    protected function setUp(): void
    {
        parent::setUp();

        foreach (range(1, 5) as $id) {
            Author::query()->create(['id' => $id, 'name' => 'Before']);
        }
    }

    public function test_cumulative_overflow_discards_tokens_and_invalidates_on_commit(): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $generationKey = $this->cacheKeys()->generation($table);
        $before = $this->cacheStore()->getRaw($generationKey);
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::transaction(function () use ($generationKey, $before): void {
            Author::query()->whereIn('id', [1, 2, 3])->update(['name' => 'At limit']);
            Author::query()->whereKey(1)->update(['name' => 'After']);
            $this->assertCount(3, $this->pending()[0]['tokens']);
            $this->assertFalse($this->pending()[0]['broad']);

            Author::query()->whereKey(4)->update(['name' => 'Overflow']);
            $this->assertSame([], $this->pending()[0]['tokens']);
            $this->assertTrue($this->pending()[0]['broad']);

            Author::query()->whereKey(5)->update(['name' => 'Still broad']);
            $this->assertSame([], $this->pending()[0]['tokens']);
            $this->assertSame($before, $this->cacheStore()->getRaw($generationKey));
        });

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($generationKey));
        $this->assertSame('After', Author::query()->findOrFail(1)->name);
        $this->assertSame([], $this->pending());
    }

    public function test_broad_invalidation_discards_previously_queued_tokens(): void
    {
        DB::transaction(function (): void {
            Author::query()->whereKey(1)->update(['name' => 'After']);
            $this->assertCount(1, $this->pending()[0]['tokens']);

            NormCache::invalidate(Author::class);
            Author::query()->whereKey(2)->update(['name' => 'After']);

            $this->assertTrue($this->pending()[0]['broad']);
            $this->assertSame([], $this->pending()[0]['tokens']);
        });
    }

    public function test_global_invalidation_replaces_pending_table_invalidations(): void
    {
        $epochKey = $this->cacheKeys()->epoch();
        $before = $this->cacheStore()->getRaw($epochKey);
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::transaction(function () use ($epochKey, $before): void {
            Author::query()->whereKey(1)->update(['name' => 'After']);
            $this->assertCount(1, $this->pending());

            Author::query()->toBase()->fromRaw('authors /* opaque write target */')->where('id', 1)->update(['name' => 'Opaque']);
            $this->assertSame([], $this->pending());
            Author::query()->whereKey(2)->update(['name' => 'After']);
            $this->assertSame([], $this->pending());
            $this->assertSame($before, $this->cacheStore()->getRaw($epochKey));
        });

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($epochKey));
        $this->assertSame('Opaque', Author::query()->findOrFail(1)->name);
    }

    private function pending(): array
    {
        $pending = (new \ReflectionProperty(Invalidator::class, 'pendingInvalidations'))
            ->getValue(app(Invalidator::class));

        return array_values($pending['testing'] ?? []);
    }
}
