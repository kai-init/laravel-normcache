<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Schema\SQLiteBuilder;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\DB;
use InvalidArgumentException;
use NormCache\Database\Connections\BuildsCachingQueries;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;

final class DependencyVectorTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Post',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
        DB::table('comments')->insert([
            'body' => 'Before',
            'commentable_type' => 'post',
            'commentable_id' => $this->postId,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_join_result_misses_after_any_dependency_version_changes(): void
    {
        $read = fn() => DB::table('posts')
            ->join('comments', 'comments.commentable_id', '=', 'posts.id')
            ->select(['posts.id', 'comments.body'])
            ->get();

        $read();
        $read();
        DB::table('comments')->where('commentable_id', $this->postId)->update(['body' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $result = $read();
        DB::disableQueryLog();

        $this->assertSame('After', $result[0]->body);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_predicate_subquery_membership_tracks_the_extra_dependency(): void
    {
        $read = fn() => DB::table('posts')
            ->whereExists(function ($query) {
                $query->from('comments')
                    ->whereColumn('comments.commentable_id', 'posts.id')
                    ->where('comments.body', 'Before');
            })
            ->get();

        $this->assertCount(1, $read());
        $this->assertCount(1, $read());

        DB::table('comments')->where('commentable_id', $this->postId)->update(['body' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $result = $read();
        DB::disableQueryLog();

        $this->assertCount(0, $result);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_where_in_subquery_is_not_cached_without_an_authoritative_dependency(): void
    {
        $read = fn() => DB::table('posts')
            ->whereIn('id', DB::table('comments')->select('commentable_id'))
            ->get();

        $this->assertCount(1, $read());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(1, $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_explicit_model_dependencies_are_additive_and_invalidate_results(): void
    {
        $author = Author::query()->firstOrFail();
        $read = fn() => Post::query()
            ->dependsOn([Author::class])
            ->whereKey($this->postId)
            ->get();

        $read();
        $read();
        Author::query()->whereKey($author->getKey())->update(['name' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_explicit_model_dependencies_use_the_active_query_connection(): void
    {
        $database = sys_get_temp_dir() . '/normcache-dependency-' . getmypid() . '.sqlite';
        copy((string) DB::connection()->getDatabaseName(), $database);
        config()->set('database.connections.tenant', [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
        ]);
        DB::purge('tenant');

        try {
            $read = fn() => Post::on('tenant')
                ->dependsOn([Author::class])
                ->selectRaw(
                    'posts.*, (select name from authors where authors.id = posts.author_id) as author_name'
                )
                ->whereKey($this->postId)
                ->firstOrFail();

            $this->assertSame('Author', $read()->author_name);
            $this->assertSame('Author', $read()->author_name);
            Author::on('tenant')->whereKey(1)->update(['name' => 'Tenant author']);

            DB::connection('tenant')->flushQueryLog();
            DB::connection('tenant')->enableQueryLog();
            $result = $read();
            DB::connection('tenant')->disableQueryLog();

            $this->assertSame('Tenant author', $result->author_name);
            $this->assertCount(1, DB::connection('tenant')->getQueryLog());
        } finally {
            DB::disconnect('tenant');
            DB::purge('tenant');
            @unlink($database);
        }
    }

    public function test_depends_on_accepts_model_and_table_dependencies_together(): void
    {
        $read = fn() => Post::query()
            ->dependsOn([Author::class, 'comments'])
            ->whereKey($this->postId)
            ->get();

        $read();
        $read();
        DB::table('comments')->where('commentable_id', $this->postId)->update(['body' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_depends_on_accepts_models_without_the_cacheable_trait(): void
    {
        $read = fn() => Author::query()
            ->dependsOn([UncachedPost::class])
            ->get();

        $read();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_depends_on_rejects_missing_model_class_names(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('does not exist');

        Post::query()->dependsOn(['App\\Models\\MissingDependency']);
    }

    public function test_depends_on_rejects_existing_non_model_classes(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('must be an Eloquent model');

        Post::query()->dependsOn([\DateTime::class]);
    }

    public function test_opaque_dependency_requires_an_explicit_authoritative_declaration(): void
    {
        $build = fn() => DB::table('posts')
            ->whereRaw(
                'exists (select 1 from comments where comments.commentable_id = posts.id)'
            );

        $build()->get();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $build()->get();
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());

        $cached = fn() => $build()->dependsOn(['comments'])->get();
        $cached();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $cached();
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_explicit_dependencies_authorize_a_hashable_derived_result_as_query_group(): void
    {
        $build = fn() => DB::table(DB::raw('(select id, title from posts) as derived'))
            ->dependsOn(['posts'])
            ->select(['id', 'title']);

        $this->assertCount(1, $build()->get());
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(1, $build()->get());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_explicit_dependency_order_does_not_change_opaque_query_identity(): void
    {
        $build = fn(array $dependencies) => DB::table(
            DB::raw('(select id, title from posts) as derived')
        )
            ->dependsOn($dependencies)
            ->select(['id', 'title']);

        $this->assertCount(1, $build(['posts', Author::class])->get());
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(1, $build([Author::class, 'posts'])->get());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_lost_select_subquery_context_requires_explicit_dependencies(): void
    {
        $base = fn() => DB::table('posts')->selectSub(
            DB::table('comments')
                ->selectRaw('count(*)')
                ->whereColumn('comments.commentable_id', 'posts.id'),
            'comment_count',
        );

        $base()->get();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $base()->get();
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());

        $declared = fn() => $base()->dependsOn(['comments'])->get();
        $declared();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $declared();
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_direct_root_calculated_projection_uses_table_local_result_caching(): void
    {
        $read = fn() => DB::table('posts')
            ->selectRaw('upper(title) as heading')
            ->where('id', $this->postId)
            ->get();

        $this->assertSame('POST', $read()[0]->heading);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('POST', $read()[0]->heading);
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_volatile_projection_is_never_cached(): void
    {
        $read = fn() => DB::table('posts')
            ->selectRaw('random() as value')
            ->where('id', $this->postId)
            ->dependsOn(['posts'])
            ->get();

        $read();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    #[DataProvider('driverTimeExpressions')]
    public function test_driver_time_expressions_are_volatile(string $expression): void
    {
        $connection = DB::connection();
        $query = DB::table('posts')->selectRaw("{$expression} as observed_at");
        $root = $this->app->make(TableIdentityResolver::class)
            ->resolve($connection, 'posts');
        $this->assertNotNull($root);

        $analysis = $this->app->make(DependencyAnalyzer::class)
            ->analyze($connection, $query, $root);

        $this->assertTrue($analysis->volatile);
    }

    public static function driverTimeExpressions(): array
    {
        return [
            ['UTC_TIMESTAMP()'],
            ['UTC_DATE()'],
            ['UTC_TIME()'],
            ['CURDATE()'],
            ['CURTIME()'],
        ];
    }

    public function test_volatile_raw_source_is_never_cached_even_with_dependencies(): void
    {
        $read = fn() => Author::query()
            ->fromRaw('(select random() as value) as sample')
            ->dependsOn([Author::class])
            ->value('value');

        $read();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_eloquent_now_function_is_never_cached(): void
    {
        DB::connection()->getPdo()->sqliteCreateFunction(
            'now',
            static fn(): string => (string) hrtime(true),
        );
        $read = fn() => Author::query()->selectRaw('now() as value')->value('value');

        $read();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_raw_ordering_subquery_is_not_cached_without_declared_dependencies(): void
    {
        $read = fn() => DB::table('posts')
            ->orderByRaw(
                '(select count(*) from comments where comments.commentable_id = posts.id) desc'
            )
            ->get();

        $read();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $read();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_eloquent_query_bypasses_when_view_metadata_lookup_fails(): void
    {
        $name = 'metadata-failure';
        $database = (string) DB::connection()->getDatabaseName();
        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
        ]);
        DB::extend($name, static fn(array $config) => new class(new PDO('sqlite:' . $database), $database, '', $config) extends SQLiteConnection
        {
            use BuildsCachingQueries;

            public function getSchemaBuilder()
            {
                return new class($this) extends SQLiteBuilder
                {
                    public function hasView($view)
                    {
                        throw new \RuntimeException('View metadata unavailable.');
                    }
                };
            }
        });
        DB::purge($name);

        try {
            $read = fn() => Post::on($name)->whereKey($this->postId)->firstOrFail();
            $this->assertSame('Post', $read()->title);

            $connection = DB::connection($name);
            $connection->flushQueryLog();
            $connection->enableQueryLog();
            $this->assertSame('Post', $read()->title);
            $connection->disableQueryLog();

            $this->assertCount(1, $connection->getQueryLog());
        } finally {
            DB::disconnect($name);
            DB::purge($name);
            DB::forgetExtension($name);
        }
    }

    public function test_database_views_require_explicit_table_dependencies(): void
    {
        DB::statement('create view post_titles as select id, title from posts');
        $this->cacheManager()->clearSchemaMetadata();

        $implicit = fn() => DB::table('post_titles')->where('id', $this->postId)->first();
        $this->assertSame('Post', $implicit()?->title);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Post', $implicit()?->title);
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());

        $explicit = fn() => DB::table('post_titles')
            ->dependsOn(['posts'])
            ->where('id', $this->postId)
            ->first();
        $this->assertSame('Post', $explicit()?->title);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Post', $explicit()?->title);
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());
        $this->assertSame([], $this->cacheKeysMatching(':r:g'));
        $this->assertCount(1, $this->cacheKeysMatching(':e:v'));

        DB::table('posts')->where('id', $this->postId)->update(['title' => 'After']);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('After', $explicit()?->title);
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());
    }
}
