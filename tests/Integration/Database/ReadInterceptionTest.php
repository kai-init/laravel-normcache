<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\SQLiteGrammar;
use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class ReadInterceptionTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Cached',
            'views' => 7,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_db_table_reads_always_execute_sql_and_create_no_cache_entry(): void
    {
        $cold = DB::table('posts')->where('id', $this->postId)->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = DB::table('posts')->where('id', $this->postId)->get();
        DB::disableQueryLog();

        $this->assertInstanceOf(\stdClass::class, $cold[0]);
        $this->assertInstanceOf(\stdClass::class, $warm[0]);
        $this->assertNotSame($cold[0], $warm[0]);
        $this->assertSame((array) $cold[0], (array) $warm[0]);
        $this->assertCount(1, DB::getQueryLog());
        $this->assertSame([], $this->cacheKeysMatching(':q:'));
    }

    public function test_opted_in_eloquent_is_cached_but_traitless_and_unmarked_reads_are_live(): void
    {
        Post::query()->whereKey($this->postId)->firstOrFail();
        UncachedPost::query()->whereKey($this->postId)->firstOrFail();
        DB::query()->from('posts')->where('id', $this->postId)->first();

        DB::flushQueryLog();
        DB::enableQueryLog();

        Post::query()->whereKey($this->postId)->firstOrFail();
        $afterCached = count(DB::getQueryLog());
        UncachedPost::query()->whereKey($this->postId)->firstOrFail();
        DB::query()->from('posts')->where('id', $this->postId)->first();

        DB::disableQueryLog();

        $this->assertSame(0, $afterCached);
        $this->assertCount(2, DB::getQueryLog());
    }

    public function test_unobserved_direct_primary_key_hit_does_not_compile_sql(): void
    {
        $connection = DB::connection();
        $originalGrammar = $connection->getQueryGrammar();
        $originalConfig = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['events'] = false;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();
        $grammar = new class($connection) extends SQLiteGrammar
        {
            public int $postSelectCompilations = 0;

            public function compileSelect(Builder $query)
            {
                if ($query->from === 'posts') {
                    $this->postSelectCompilations++;
                }

                return parent::compileSelect($query);
            }
        };
        $connection->setQueryGrammar($grammar);

        try {
            Post::query()->toBase()->where('id', $this->postId)->first();
            $grammar->postSelectCompilations = 0;

            Post::query()->toBase()->where('id', $this->postId)->first();

            $this->assertSame(0, $grammar->postSelectCompilations);
        } finally {
            $connection->setQueryGrammar($originalGrammar);
            $this->app->instance(CacheConfig::class, $originalConfig);
            $this->app->forgetScopedInstances();
        }
    }

    public function test_before_callbacks_affect_identity_once_and_after_callbacks_see_hits(): void
    {
        $beforeCalls = 0;
        $afterCalls = 0;

        $run = function () use (&$beforeCalls, &$afterCalls) {
            return Post::query()->toBase()
                ->beforeQuery(function ($query) use (&$beforeCalls) {
                    $beforeCalls++;
                    $query->where('views', 7);
                })
                ->afterQuery(function ($rows) use (&$afterCalls) {
                    $afterCalls++;

                    return $rows;
                })
                ->where('id', $this->postId)
                ->get();
        };

        $run();
        $run();

        $this->assertSame(2, $beforeCalls);
        $this->assertSame(2, $afterCalls);
    }

    public function test_exists_uses_the_cache_and_conditional_variants_delegate_to_it(): void
    {
        $query = fn() => Post::query()->toBase()->where('id', $this->postId);

        $this->assertTrue($query()->exists());

        DB::flushQueryLog();
        DB::enableQueryLog();

        $this->assertTrue($query()->exists());
        $this->assertFalse($query()->doesntExist());
        $this->assertTrue($query()->existsOr(fn() => false));

        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_exists_cannot_poison_the_canonical_primary_key_row(): void
    {
        $query = fn() => Post::query()->toBase()->where('id', $this->postId);

        $this->assertTrue($query()->exists());
        $row = $query()->first();

        $this->assertInstanceOf(\stdClass::class, $row);
        $this->assertSame('Cached', $row->title);
    }

    public function test_count_cannot_poison_the_canonical_primary_key_row(): void
    {
        $query = fn() => Post::query()->toBase()->where('id', $this->postId);

        $this->assertSame(1, $query()->count());
        $row = $query()->first();

        $this->assertInstanceOf(\stdClass::class, $row);
        $this->assertSame('Cached', $row->title);
    }

    public function test_explicit_and_execution_safety_bypasses_remain_live(): void
    {
        Post::query()->toBase()->where('id', $this->postId)->get();

        DB::flushQueryLog();
        DB::enableQueryLog();

        Post::query()->toBase()->where('id', $this->postId)->withoutCache()->get();
        Post::query()->toBase()->where('id', $this->postId)->useWritePdo()->get();
        DB::transaction(fn() => Post::query()->toBase()->where('id', $this->postId)->get());

        DB::disableQueryLog();

        $this->assertCount(3, DB::getQueryLog());
    }
}
