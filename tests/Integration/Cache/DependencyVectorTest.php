<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\DB;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\SqlVolatilityScanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\AbstractComment;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use PHPUnit\Framework\Attributes\DataProvider;

trait SplitsPipelineAroundInvalidation
{
    private bool $intercepted = false;

    public function command($method, array $parameters = [])
    {
        if ($method === 'pipeline') {
            return $this->pipeline($parameters[0] ?? null);
        }

        return parent::command($method, $parameters);
    }

    public function pipeline(?callable $callback = null)
    {
        $recorder = new class
        {
            /** @var list<array{0: string, 1: array<int, mixed>}> */
            public array $calls = [];

            public function __call(string $method, array $arguments): static
            {
                $this->calls[] = [$method, $arguments];

                return $this;
            }
        };

        $callback($recorder);
        $replies = [];

        foreach ($recorder->calls as $index => [$method, $arguments]) {
            $replies[] = $this->command($method, $arguments);

            if ($index === 0 && !$this->intercepted) {
                $this->intercepted = true;
                ($this->afterFirstCommand)();
            }
        }

        return $replies;
    }
}

final class PostTitlesView extends Model
{
    use Cacheable;

    protected $table = 'post_titles';
}

final class DependencyVectorTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) RawPost::query()->toBase()->insertGetId([
            'title' => 'Post',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
        Comment::query()->toBase()->insert([
            'body' => 'Before',
            'commentable_type' => 'post',
            'commentable_id' => $this->postId,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_join_result_misses_after_any_dependency_version_changes(): void
    {
        $read = fn() => RawPost::query()->toBase()
            ->join('comments', 'comments.commentable_id', '=', 'posts.id')
            ->select(['posts.id', 'comments.body'])
            ->get();

        $read();
        $read();
        Comment::query()->toBase()->where('commentable_id', $this->postId)->update(['body' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $result = $read();
        DB::disableQueryLog();

        $this->assertSame('After', $result[0]->body);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_predicate_subquery_membership_tracks_the_extra_dependency(): void
    {
        $read = fn() => RawPost::query()->toBase()
            ->whereExists(function ($query) {
                $query->from('comments')
                    ->whereColumn('comments.commentable_id', 'posts.id')
                    ->where('comments.body', 'Before');
            })
            ->get();

        $this->assertCount(1, $read());
        $this->assertCount(1, $read());

        Comment::query()->toBase()->where('commentable_id', $this->postId)->update(['body' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $result = $read();
        DB::disableQueryLog();

        $this->assertCount(0, $result);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_where_in_subquery_requires_declared_dependencies(): void
    {
        $build = fn(bool $declared = false) => RawPost::query()->toBase()
            ->whereIn('id', Comment::query()->toBase()->select('commentable_id'))
            ->when($declared, fn($query) => $query->dependsOn(['comments']));

        $this->bypassContract(
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $build(true)->get()->map(static fn($row): array => (array) $row),
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            mutate: fn() => Comment::query()->toBase()
                ->where('commentable_id', $this->postId)
                ->delete(),
        );
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
            $read = fn() => RawPost::on('tenant')
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
        Comment::query()->toBase()->where('commentable_id', $this->postId)->update(['body' => 'After']);

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
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('does not exist');

        Post::query()->dependsOn(['App\\Models\\MissingDependency']);
    }

    public function test_depends_on_rejects_existing_non_model_classes(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('must be an Eloquent model');

        Post::query()->dependsOn([\DateTime::class]);
    }

    public function test_raw_predicate_subquery_requires_declared_dependencies(): void
    {
        $build = fn(bool $declared = false) => RawPost::query()->toBase()
            ->whereRaw(
                'exists (select 1 from comments where comments.commentable_id = posts.id)'
            )
            ->when($declared, fn($query) => $query->dependsOn(['comments']));

        $this->bypassContract(
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $build(true)->get()->map(static fn($row): array => (array) $row),
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            mutate: fn() => Comment::query()->toBase()
                ->where('commentable_id', $this->postId)
                ->delete(),
        );
    }

    public function test_derived_raw_subquery_requires_an_explicit_authoritative_declaration(): void
    {
        $build = fn() => RawPost::query()->toBase()
            ->whereRaw(
                'exists (select 1 from (select commentable_id from comments) as recent_comments where recent_comments.commentable_id = posts.id)'
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

    public function test_unresolvable_declaration_does_not_authorize_an_opaque_query(): void
    {
        $build = fn() => RawPost::query()->toBase()
            ->whereRaw(
                'exists (select 1 from comments where comments.commentable_id = posts.id)'
            )
            ->dependsOn([AbstractComment::class])
            ->get();

        $build();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $build();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_unresolvable_declaration_bypasses_an_otherwise_cacheable_query(): void
    {
        $build = fn() => RawPost::query()->toBase()
            ->where('id', $this->postId)
            ->dependsOn([Author::class, AbstractComment::class])
            ->get();

        $build();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $build();
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_unresolvable_declaration_never_serves_stale_rows(): void
    {
        $build = fn() => RawPost::query()->toBase()
            ->whereRaw('exists (select 1 from comments where comments.body = ?)', ['Before'])
            ->dependsOn([AbstractComment::class])
            ->get();

        $this->assertCount(1, $build());
        Comment::query()->toBase()->where('commentable_id', $this->postId)->update(['body' => 'After']);

        $this->assertCount(0, $build());
    }

    public function test_declared_table_dependencies_are_trusted_without_schema_queries(): void
    {
        $build = fn() => RawPost::query()->toBase()
            ->where('id', $this->postId)
            ->dependsOn(['definitely_missing_table'])
            ->get();

        $build();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $build();
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_unresolvable_declaration_is_reported_as_incomplete(): void
    {
        $connection = DB::connection();
        $query = RawPost::query()->toBase()->dependsOn(['comments', AbstractComment::class]);
        $root = $this->app->make(TableIdentityResolver::class)
            ->resolve($connection, 'posts');
        $this->assertNotNull($root);

        $analysis = $this->app->make(DependencyAnalyzer::class)
            ->analyze($connection, $query);

        $this->assertSame('unresolvable_declared_dependency', $analysis->bypassReason);
    }

    public function test_explicit_dependencies_authorize_a_hashable_derived_result_as_query_group(): void
    {
        $build = fn() => RawPost::query()->toBase()
            ->from(DB::raw('(select id, title from posts) as derived'))
            ->dependsOn(['posts'])
            ->select(['id', 'title']);

        $this->assertCount(1, $build()->get());
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(1, $build()->get());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_query_group_invalidation_between_payload_and_state_reads_cannot_serve_stale_data(): void
    {
        $read = fn() => RawPost::query()->toBase()
            ->from(DB::raw('(select id, title from posts) as derived'))
            ->dependsOn(['posts'])
            ->where('id', $this->postId)
            ->first();

        $this->assertSame('Post', $read()?->title);

        $store = $this->cacheStore();
        $connectionProperty = (new \ReflectionClass($store))->getProperty('connection');
        $connection = $connectionProperty->getValue($store);
        $invalidate = function (): void {
            RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'After']);
        };

        // Redis lets another client's write land between two commands of one
        // pipeline; running them separately reproduces that worst case.
        if ($connection instanceof PhpRedisConnection) {
            $interceptingConnection = new class($connection->client(), $invalidate) extends PhpRedisConnection
            {
                use SplitsPipelineAroundInvalidation;

                public function __construct(
                    mixed $client,
                    private \Closure $afterFirstCommand,
                ) {
                    parent::__construct($client);
                }
            };
        } else {
            $this->assertInstanceOf(PredisConnection::class, $connection);
            $interceptingConnection = new class($connection->client(), $invalidate) extends PredisConnection
            {
                use SplitsPipelineAroundInvalidation;

                public function __construct(
                    mixed $client,
                    private \Closure $afterFirstCommand,
                ) {
                    parent::__construct($client);
                }
            };
        }

        $connectionProperty->setValue($store, $interceptingConnection);

        try {
            $result = $read();
        } finally {
            $connectionProperty->setValue($store, $connection);
        }

        $this->assertSame('After', $result?->title);
    }

    public function test_explicit_dependency_order_does_not_change_opaque_query_identity(): void
    {
        $build = fn(array $dependencies) => RawPost::query()->toBase()
            ->from(DB::raw('(select id, title from posts) as derived'))
            ->dependsOn($dependencies)
            ->select(['id', 'title']);

        $this->assertCount(1, $build(['posts', Author::class])->get());
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(1, $build([Author::class, 'posts'])->get());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_select_subquery_context_is_captured_before_compilation(): void
    {
        $base = fn() => RawPost::query()->toBase()->selectSub(
            Comment::query()->toBase()
                ->selectRaw('count(*)')
                ->whereColumn('comments.commentable_id', 'posts.id'),
            'comment_count',
        );

        $this->assertSame(1, (int) $base()->first()->comment_count);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame(1, (int) $base()->first()->comment_count);
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());

        Comment::query()->toBase()->insert([
            'body' => 'Second',
            'commentable_type' => 'post',
            'commentable_id' => $this->postId,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->assertSame(
            2,
            (int) $base()->first()->comment_count,
            'a captured select subquery must invalidate with its own table',
        );

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
        $read = fn() => RawPost::query()->toBase()
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
        $read = fn() => RawPost::query()->toBase()
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

    public function test_random_bytes_projection_is_never_cached(): void
    {
        $this->createSqliteFunction(
            'random_bytes',
            static fn(int $length): string => bin2hex(\random_bytes($length)),
        );
        $read = fn(): string => (string) RawPost::query()->toBase()
            ->selectRaw('random_bytes(16) as value')
            ->where('id', $this->postId)
            ->value('value');

        $first = $read();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $second = $read();
        DB::disableQueryLog();

        $this->assertNotSame($first, $second);
        $this->assertCount(1, DB::getQueryLog());
    }

    #[DataProvider('previouslyUncoveredVolatileExpressions')]
    public function test_connection_and_random_state_expressions_are_volatile(string $expression): void
    {
        $query = RawPost::query()->toBase()->selectRaw("{$expression} as observed_value");
        $this->assertTrue(
            $this->app->make(SqlVolatilityScanner::class)->isVolatile($query->toSql()),
        );
    }

    public static function previouslyUncoveredVolatileExpressions(): array
    {
        return [
            ['RANDOM_BYTES(16)'],
            ['GEN_RANDOM_BYTES(16)'],
            ['CRYPT_GEN_RANDOM(16)'],
            ['CURRENT_ROLE'],
            ['USER()'],
            ['DATABASE()'],
            ['CURRENT_SCHEMA()'],
        ];
    }

    #[DataProvider('driverTimeExpressions')]
    public function test_driver_time_expressions_are_volatile(string $expression): void
    {
        $query = RawPost::query()->toBase()->selectRaw("{$expression} as observed_at");
        $this->assertTrue(
            $this->app->make(SqlVolatilityScanner::class)->isVolatile($query->toSql()),
        );
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
        $this->createSqliteFunction(
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

    private function createSqliteFunction(string $name, callable $callback): void
    {
        $pdo = DB::connection()->getPdo();

        if (method_exists($pdo, 'createFunction')) {
            $pdo->createFunction($name, $callback);
        } elseif (method_exists($pdo, 'sqliteCreateFunction')) {
            /** @var \PDO $pdo */
            $pdo->sqliteCreateFunction($name, $callback);
        }
    }

    public function test_raw_ordering_subquery_requires_declared_dependencies(): void
    {
        $build = fn(bool $declared = false) => RawPost::query()->toBase()
            ->orderByRaw(
                '(select count(*) from comments where comments.commentable_id = posts.id) desc'
            )
            ->when($declared, fn($query) => $query->dependsOn(['comments']));

        $this->bypassContract(
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $build(true)->get()->map(static fn($row): array => (array) $row),
            fn() => $build()->get()->map(static fn($row): array => (array) $row),
            mutate: fn() => Comment::query()->toBase()
                ->where('commentable_id', $this->postId)
                ->delete(),
        );
    }

    public function test_unnamed_custom_connection_bypasses_without_a_source_scope(): void
    {
        $name = 'unnamed-source';
        $database = (string) DB::connection()->getDatabaseName();
        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
        ]);
        DB::extend($name, static fn(array $config) => new class(new \PDO('sqlite:' . $database), $database, '', $config) extends SQLiteConnection {});
        DB::purge($name);

        try {
            $read = fn() => RawPost::on($name)->whereKey($this->postId)->firstOrFail();
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

    public function test_database_views_use_explicit_physical_dependencies(): void
    {
        DB::statement('create view post_titles as select id, title from posts');

        $explicit = fn() => PostTitlesView::query()->toBase()
            ->dependsOn(['posts'])
            ->where('id', $this->postId)
            ->first();
        $this->assertSame('Post', $explicit()?->title);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Post', $explicit()?->title);
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());

        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'After']);
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('After', $explicit()?->title);
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());
    }
}
