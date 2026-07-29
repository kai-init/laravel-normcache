<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use NormCache\Database\QueryBuilder;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;

final class BuilderOriginTest extends TestCase
{
    public function test_cacheable_trait_exposes_metadata_only_for_opted_in_eloquent_models(): void
    {
        $cached = Post::query()->getQuery();
        $uncached = UncachedPost::query()->getQuery();

        $this->assertInstanceOf(QueryBuilder::class, $cached);
        $this->assertSame(Post::class, $cached->modelClass());
        $this->assertSame('id', $cached->primaryKey()?->column);
        $this->assertSame('integer', $cached->primaryKey()?->family);
        $this->assertSame('deleted_at', $cached->deletedAtColumn());

        $this->assertInstanceOf(QueryBuilder::class, $uncached);
        $this->assertNull($uncached->modelClass());
        $this->assertNull($uncached->primaryKey());
        $this->assertNull($uncached->deletedAtColumn());
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
            $this->assertInstanceOf(QueryBuilder::class, $builder);
            $this->assertSame($connection, $builder->getConnection());
        }
    }
}
