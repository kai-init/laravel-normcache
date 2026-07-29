<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\TestCase;

final class PrimaryKeyResolverTest extends TestCase
{
    public function test_tables_with_the_same_key_shape_share_one_metadata_instance(): void
    {
        $resolver = $this->app->make(PrimaryKeyResolver::class);
        $connection = DB::connection();
        $tables = $this->app->make(TableIdentityResolver::class);

        $posts = $tables->resolve($connection, 'posts');
        $authors = $tables->resolve($connection, 'authors');
        $this->assertNotNull($posts);
        $this->assertNotNull($authors);

        $postsKey = $resolver->resolve(DB::table('posts'), $connection, $posts);
        $authorsKey = $resolver->resolve(DB::table('authors'), $connection, $authors);

        $this->assertNotNull($postsKey);
        $this->assertNotNull($authorsKey);
        $this->assertSame('id', $postsKey->column);
        $this->assertSame($postsKey->family, $authorsKey->family);
        $this->assertSame(
            $postsKey,
            $authorsKey,
            'tables with an identical key shape must share one metadata instance',
        );
    }

    public function test_clearing_one_connection_leaves_other_connections_memoized(): void
    {
        $resolver = $this->app->make(PrimaryKeyResolver::class);
        $connection = DB::connection();
        $posts = $this->app->make(TableIdentityResolver::class)->resolve($connection, 'posts');
        $this->assertNotNull($posts);

        $this->assertNotNull($resolver->resolve(DB::table('posts'), $connection, $posts));

        $resolver->clear('some-other-connection');

        $this->assertNotNull($resolver->resolve(DB::table('posts'), $connection, $posts));

        $resolver->clear($posts->connection);

        $this->assertNotNull($resolver->resolve(DB::table('posts'), $connection, $posts));
    }
}
