<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Facades\NormCache;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\QueryIdentity;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class ProjectionFallbackTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Canonical',
            'views' => 10,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_falls_back_to_warm_row_cache_without_db_query(): void
    {
        DB::table('posts')->orderBy('id')->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_falls_through_to_database_when_row_cache_is_cold(): void
    {
        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertNotEmpty(DB::getQueryLog());
    }

    public function test_fallback_respects_soft_delete_visibility(): void
    {
        Post::query()->whereKey($this->postId)->delete();
        Post::withTrashed()->findOrFail($this->postId);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $default = Post::query()->select('title')->find($this->postId);
        $trashedOnly = Post::onlyTrashed()->select('title')->find($this->postId);
        DB::disableQueryLog();

        $this->assertNull($default);
        $this->assertSame('Canonical', $trashedOnly?->title);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_fallback_bypasses_on_extra_predicates(): void
    {
        DB::table('posts')->orderBy('id')->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')
            ->where('id', $this->postId)
            ->where('published', false)
            ->select('title')
            ->first();
        DB::disableQueryLog();

        $this->assertNull($row);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_fallback_reflects_precise_write(): void
    {
        DB::table('posts')->orderBy('id')->get();
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Updated']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Updated', $row->title);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_fallback_serves_unrelated_rows_after_collateral_invalidation(): void
    {
        $authorId = DB::table('posts')->where('id', $this->postId)->value('author_id');
        $secondId = (int) DB::table('posts')->insertGetId([
            'title' => 'Second',
            'views' => 0,
            'published' => true,
            'author_id' => $authorId,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        DB::table('posts')->orderBy('id')->get();
        DB::table('posts')->where('id', $secondId)->update(['title' => 'Second Updated']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_fallback_misses_after_global_epoch_flush(): void
    {
        DB::table('posts')->orderBy('id')->get();
        NormCache::flushAll();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_fallback_declines_when_projected_column_is_missing(): void
    {
        DB::table('posts')->orderBy('id')->get();

        $codec = $this->app->make(RawResultCodec::class);
        $epoch = $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0';
        $rowKey = $this->cacheKeysMatching(':r:g')[0] ?? null;
        $this->assertIsString($rowKey);

        $incomplete = (object) ['id' => $this->postId];
        $this->cacheStore()->setRaw($rowKey, $codec->encodeRow($incomplete, $epoch), 3600);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertCount(1, DB::getQueryLog());
        $this->assertNull($this->cacheStore()->getRaw($rowKey));
    }

    public function test_aliased_source_does_not_accept_original_table_projection_qualifier(): void
    {
        DB::table('posts')->orderBy('id')->get();

        $query = fn(bool $cached) => DB::table('posts as p')
            ->when(!$cached, fn($builder) => $builder->withoutCache())
            ->where('p.id', $this->postId)
            ->select('posts.title')
            ->first();

        $this->assertQueryFails(fn() => $query(false));
        $this->assertQueryFails(fn() => $query(true));
    }

    public function test_aliased_source_does_not_accept_original_table_wildcard(): void
    {
        DB::table('posts')->orderBy('id')->get();

        $query = fn(bool $cached) => DB::table('posts as p')
            ->when(!$cached, fn($builder) => $builder->withoutCache())
            ->where('p.id', $this->postId)
            ->select('posts.*')
            ->first();

        $this->assertQueryFails(fn() => $query(false));
        $this->assertQueryFails(fn() => $query(true));
    }

    public function test_unrelated_primary_key_qualifier_does_not_use_row_fallback(): void
    {
        DB::table('posts')->orderBy('id')->get();

        $query = fn(bool $cached) => DB::table('posts')
            ->when(!$cached, fn($builder) => $builder->withoutCache())
            ->where('authors.id', $this->postId)
            ->select('title')
            ->first();

        $this->assertQueryFails(fn() => $query(false));
        $this->assertQueryFails(fn() => $query(true));
    }

    public function test_contradictory_soft_delete_predicates_do_not_use_row_fallback(): void
    {
        Post::query()->whereKey($this->postId)->delete();
        Post::withTrashed()->findOrFail($this->postId);

        $native = Post::query()
            ->whereNotNull('deleted_at')
            ->select('title')
            ->withoutCache()
            ->find($this->postId);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $cached = Post::query()
            ->whereNotNull('deleted_at')
            ->select('title')
            ->find($this->postId);
        DB::disableQueryLog();

        $this->assertNull($native);
        $this->assertNull($cached);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_canonical_row_repair_is_reported_separately_when_this_process_queries_database(): void
    {
        DB::table('posts')->orderBy('id')->get();
        $this->cacheStore()->delete($this->postRowKey());

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class, QueryCacheRepaired::class]);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = DB::table('posts')->orderBy('id')->get();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $rows->firstWhere('id', $this->postId)->title);
        $this->assertCount(1, DB::getQueryLog());
        Event::assertDispatched(
            QueryCacheRepaired::class,
            static fn(QueryCacheRepaired $event): bool => $event->reason === 'row_repair',
        );
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_corrupt_canonical_row_is_removed_when_projection_fallback_declines(): void
    {
        DB::table('posts')->orderBy('id')->get();
        $rowKey = $this->postRowKey();
        $this->cacheStore()->setRaw($rowKey, 'not-a-valid-row-payload', 3600);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->select('title')->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertCount(1, DB::getQueryLog());
        $this->assertNull($this->cacheStore()->getRaw($rowKey));
    }

    public function test_retry_preserves_row_fallback_hit_reason(): void
    {
        if (
            !function_exists('pcntl_fork')
            || !class_exists(\Redis::class)
            || env('REDIS_CLUSTER') === true
            || env('REDIS_CLUSTER') === 'true'
        ) {
            $this->markTestSkipped('Requires pcntl and standalone PhpRedis.');
        }

        $query = DB::table('posts')->where('id', $this->postId)->select('title')->limit(1);
        $connection = $query->getConnection();
        $table = $this->app->make(TableIdentityResolver::class)->resolve($connection, $query->from);
        $this->assertNotNull($table);
        $analysis = $this->app->make(DependencyAnalyzer::class)->analyze($connection, $query, $table);
        $primaryKey = $this->app->make(PrimaryKeyResolver::class)->resolve($query, $connection, $table);
        $plan = $this->app->make(QueryPlanner::class)->plan(
            $query,
            $table,
            $primaryKey,
            $analysis->tables,
        );
        $identity = $this->app->make(QueryIdentity::class);
        $namespace = $identity->namespace($query->normCacheTag());
        $queryHash = $identity->hash(
            route: $plan->route,
            rootHash: $table->hash,
            dependencyHashes: array_map(static fn($dependency): string => $dependency->hash, $analysis->tables),
            sql: $query->toSql(),
            bindings: $connection->prepareBindings($query->getBindings()),
            namespace: $namespace,
            operation: 'select',
        );
        $version = $this->cacheStore()->getRaw($this->cacheKeys()->version($table)) ?? '0';
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $epoch = $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0';
        $token = str_repeat('a', 32);
        $buildKey = $this->cacheKeys()->resultBuild($table, $version, $namespace, $queryHash);
        $wakeKey = $this->cacheKeys()->wake($table, 'e', $queryHash, $token);
        $rowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $this->postId);
        $payload = $this->app->make(RawResultCodec::class)->encodeRow(
            (object) ['id' => $this->postId, 'title' => 'Canonical'],
            $epoch,
        );
        $this->assertTrue($this->cacheStore()->setNxEx($buildKey, $token, 5));

        $pid = pcntl_fork();
        $this->assertNotSame(-1, $pid);

        if ($pid === 0) {
            usleep(50_000);
            $redis = new \Redis;
            $redis->connect((string) env('REDIS_HOST', '127.0.0.1'), (int) env('REDIS_PORT', 6379));
            $redis->select(15);
            $redis->setex($rowKey, 3600, $payload);
            $redis->lPush($wakeKey, '1');
            $redis->expire($wakeKey, 10);
            $redis->del($buildKey);
            $redis->close();
            exit(0);
        }

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $started = microtime(true);
        $row = $query->first();
        $elapsed = microtime(true) - $started;
        DB::disableQueryLog();
        pcntl_waitpid($pid, $status);

        $this->assertSame('Canonical', $row->title);
        $this->assertSame([], DB::getQueryLog());
        $this->assertGreaterThan(0.02, $elapsed);
        Event::assertDispatched(
            QueryCacheHit::class,
            static fn(QueryCacheHit $event): bool => $event->reason === 'row_cache_fallback',
        );
        Event::assertDispatched(QueryCacheMiss::class);
    }

    public function test_fallback_is_reported_as_cache_hit(): void
    {
        DB::table('posts')->orderBy('id')->get();

        Event::fake([QueryCacheHit::class, QueryCacheMiss::class]);

        DB::table('posts')->where('id', $this->postId)->select('title')->first();

        Event::assertDispatched(
            QueryCacheHit::class,
            static fn(QueryCacheHit $event): bool => $event->reason === 'row_cache_fallback',
        );
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    private function postRowKey(): string
    {
        foreach ($this->cacheKeysMatching(':r:g') as $key) {
            if (str_ends_with($key, ':i:' . $this->postId)) {
                return $key;
            }
        }

        throw new \RuntimeException('Post canonical row key was not found.');
    }

    private function assertQueryFails(callable $query): void
    {
        try {
            $query();
            $this->fail('Expected the database query to fail.');
        } catch (QueryException) {
            $this->addToAssertionCount(1);
        }
    }
}
