<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use NormCache\Database\CachingQueryBuilder;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;

final class BuilderOriginTest extends TestCase
{
    public function test_connection_table_marks_db_table_origin(): void
    {
        $builder = DB::table('posts');

        $this->assertInstanceOf(CachingQueryBuilder::class, $builder);
        $this->assertSame(CachingQueryBuilder::ORIGIN_DB_TABLE, $builder->normCacheOrigin());
    }

    public function test_direct_connection_query_remains_unmarked(): void
    {
        $builder = DB::query()->from('posts');

        $this->assertInstanceOf(CachingQueryBuilder::class, $builder);
        $this->assertNull($builder->normCacheOrigin());
    }

    public function test_cacheable_trait_marks_only_opted_in_eloquent_models(): void
    {
        $cached = Post::query()->getQuery();
        $uncached = UncachedPost::query()->getQuery();

        $this->assertInstanceOf(CachingQueryBuilder::class, $cached);
        $this->assertSame(CachingQueryBuilder::ORIGIN_CACHEABLE_MODEL, $cached->normCacheOrigin());
        $this->assertSame(Post::class, $cached->normCacheModelClass());

        $this->assertInstanceOf(CachingQueryBuilder::class, $uncached);
        $this->assertNull($uncached->normCacheOrigin());
        $this->assertNull($uncached->normCacheModelClass());
    }

    public function test_supported_query_entry_points_share_a_concrete_laravel_connection(): void
    {
        $connection = DB::connection();
        $builders = [
            DB::query(),
            DB::table('posts'),
            Post::query()->getQuery(),
            UncachedPost::query()->getQuery(),
        ];

        $this->assertInstanceOf(Connection::class, $connection);

        foreach ($builders as $builder) {
            $this->assertSame($connection, $builder->getConnection());
        }
    }
}
