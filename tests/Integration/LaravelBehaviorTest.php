<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\NewFromBuilderOverridingPost;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use PDO;

final class LaravelBehaviorTest extends TestCase
{
    public function test_pagination_eager_loading_and_pivot_hydration_replay_without_warm_sql(): void
    {
        $author = Author::query()->create(['name' => 'Author']);
        $post = Post::query()->create([
            'title' => 'Post',
            'views' => 12,
            'published' => true,
            'author_id' => $author->getKey(),
        ]);
        Comment::query()->create([
            'body' => 'Comment',
            'commentable_type' => Post::class,
            'commentable_id' => $post->getKey(),
        ]);
        $tag = Tag::query()->create(['name' => 'Tag']);
        $post->tags()->attach($tag);

        $load = fn() => Post::query()
            ->with(['author', 'comments', 'tags'])
            ->paginate(10);
        $cold = $load();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = $load();
        DB::disableQueryLog();

        $this->assertSame($cold->total(), $warm->total());
        $this->assertSame(
            $cold->getCollection()->toArray(),
            $warm->getCollection()->toArray(),
        );
        $this->assertSame($post->getKey(), $warm[0]->tags[0]->pivot->taggable_id);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_cached_rows_still_use_laravels_new_from_builder_hydration_path(): void
    {
        $author = Author::query()->create(['name' => 'Author']);
        $post = NewFromBuilderOverridingPost::query()->create([
            'title' => 'Hydrated',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
        ]);
        NewFromBuilderOverridingPost::$newFromBuilderCalls = 0;

        NewFromBuilderOverridingPost::query()->findOrFail($post->getKey());
        $afterCold = NewFromBuilderOverridingPost::$newFromBuilderCalls;
        NewFromBuilderOverridingPost::query()->findOrFail($post->getKey());

        $this->assertSame(1, $afterCold);
        $this->assertSame(2, NewFromBuilderOverridingPost::$newFromBuilderCalls);
    }

    public function test_mutating_a_connection_database_changes_its_table_identity(): void
    {
        $author = Author::query()->create(['name' => 'Author']);
        $post = Post::query()->create([
            'title' => 'Tenant one',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
        ]);
        $connection = DB::connection();
        $originalDatabase = (string) $connection->getDatabaseName();
        $originalPdo = $connection->getPdo();
        $resolver = $this->app->make(TableIdentityResolver::class);
        $firstIdentity = $resolver->resolve($connection, 'posts');
        $this->assertSame('Tenant one', DB::table('posts')->where('id', $post->getKey())->first()?->title);

        $database = sys_get_temp_dir() . '/normcache-tenant-' . getmypid() . '.sqlite';
        copy($originalDatabase, $database);
        $tenantPdo = new PDO('sqlite:' . $database);
        $tenantPdo->exec("update posts set title = 'Tenant two' where id = {$post->getKey()}");

        try {
            $connection->setDatabaseName($database);
            $connection->setPdo($tenantPdo);
            $resolver->clear('testing');
            $secondIdentity = $resolver->resolve($connection, 'posts');

            $this->assertNotSame($firstIdentity?->hash, $secondIdentity?->hash);
            $this->assertSame(
                'Tenant two',
                DB::table('posts')->withoutCache()->where('id', $post->getKey())->first()?->title,
            );
            $this->assertSame(
                'Tenant two',
                DB::table('posts')->where('id', $post->getKey())->first()?->title,
            );
        } finally {
            $connection->setDatabaseName($originalDatabase);
            $connection->setPdo($originalPdo);
            @unlink($database);
        }
    }
}
