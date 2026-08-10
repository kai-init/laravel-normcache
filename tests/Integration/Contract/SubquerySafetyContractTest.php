<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class SubquerySafetyContractTest extends TestCase
{
    public function test_wildcard_from_sub_never_publishes_derived_rows_as_model_rows(): void
    {
        $author = Author::create(['name' => 'Real Author']);
        Post::create([
            'title' => 'Derived Post',
            'author_id' => $author->id,
            'views' => 10,
            'published' => true,
        ]);

        $derived = static fn() => Author::query()
            ->fromSub(Post::query(), 'derived')
            ->dependsOn([Post::class])
            ->get();

        $this->assertColdCacheMiss($derived);
        $this->assertWarmCacheHit($derived);

        $cached = null;
        $this->assertColdCacheMiss(function () use ($author, &$cached) {
            return $cached = Author::find($author->id);
        });

        $this->assertSame('Real Author', $cached?->name);
        $this->assertArrayNotHasKey('title', $cached?->getAttributes() ?? []);
        $this->assertWarmCacheHit(static fn() => Author::find($author->id));
    }

    public function test_query_builder_from_sub_requires_a_declared_root(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $query = static fn(bool $declared = false) => Author::query()->toBase()
            ->fromSub(
                Author::query()->toBase()->select([
                    'id',
                    'name',
                    'country_id',
                    'created_at',
                    'updated_at',
                ]),
                'derived_authors',
            )
            ->when($declared, fn($query) => $query->dependsOn(['authors']))
            ->orderBy('derived_authors.id')
            ->get()
            ->map(static fn(\stdClass $row): array => (array) $row)
            ->values()
            ->all();

        $this->bypassContract(
            fn() => $query(),
            fn() => $query(),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            fn() => $query(true),
            fn() => $query(),
            mutate: fn() => Author::whereKey($alice->id)->update(['name' => 'Alice Updated']),
        );
    }

    public function test_comma_join_raw_subquery_fails_open_until_dependencies_are_declared(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Comment::create([
            'body' => 'First',
            'commentable_type' => Author::class,
            'commentable_id' => $author->id,
        ]);
        Tag::create(['name' => 'one']);

        $query = static fn() => Author::query()
            ->selectRaw('(select count(*) from comments, tags) as dependency_count')
            ->whereKey($author->id)
            ->first();
        $native = static fn() => Author::withoutCache()
            ->selectRaw('(select count(*) from comments, tags) as dependency_count')
            ->whereKey($author->id)
            ->first();

        $this->bypassContract($query, $native, reason: 'unidentifiable_dependency');

        $cached = static fn() => Author::query()
            ->selectRaw('(select count(*) from comments, tags) as dependency_count')
            ->dependsOn(['comments', 'tags'])
            ->whereKey($author->id)
            ->first();

        $this->contract(
            $cached,
            $native,
            mutate: static fn() => Tag::create(['name' => 'two']),
        );
    }

    public function test_raw_projection_cannot_steal_a_structured_subquery_capture(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create([
            'title' => 'Post',
            'author_id' => $author->id,
            'published' => true,
        ]);

        $build = static function (bool $native, bool $declared = false) use ($author) {
            $posts = Post::query()
                ->selectRaw('count(*)')
                ->whereColumn('posts.author_id', 'authors.id');
            $postsSql = $posts->toSql();
            $query = $native ? Author::withoutCache() : Author::query();

            return $query
                ->addSelect(['post_count' => $posts])
                ->selectRaw(
                    "(select count(*) from comments where exists ({$postsSql})) as comment_count"
                )
                ->when($declared, fn($query) => $query->dependsOn([Comment::class]))
                ->whereKey($author->id)
                ->first();
        };

        $this->bypassContract(
            static fn() => $build(false),
            static fn() => $build(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            static fn() => $build(false, true),
            static fn() => $build(true),
            mutate: static fn() => Comment::create([
                'body' => 'New comment',
                'commentable_type' => Post::class,
                'commentable_id' => $post->id,
            ]),
        );
    }

    public function test_nested_view_requires_physical_base_table_dependencies(): void
    {
        if (DB::connection()->getDriverName() !== 'sqlite') {
            $this->markTestSkipped('Portable view-safety contract currently uses SQLite syntax.');
        }

        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        DB::statement('create view post_titles as select id, title from posts');

        try {
            $query = static fn() => Author::query()
                ->selectRaw('(select count(*) from post_titles) as post_count')
                ->whereKey($author->id)
                ->first();
            $native = static fn() => Author::withoutCache()
                ->selectRaw('(select count(*) from post_titles) as post_count')
                ->whereKey($author->id)
                ->first();

            $this->bypassContract($query, $native, reason: 'unidentifiable_dependency');

            $physical = static fn() => Author::query()
                ->selectRaw('(select count(*) from post_titles) as post_count')
                ->dependsOn(['posts'])
                ->whereKey($author->id)
                ->first();

            $this->contract(
                $physical,
                $native,
                mutate: static fn() => Post::create([
                    'title' => 'P2',
                    'author_id' => $author->id,
                ]),
            );
        } finally {
            DB::statement('drop view if exists post_titles');
        }
    }

    public function test_builder_backed_nested_view_supports_physical_base_table_dependencies(): void
    {
        if (DB::connection()->getDriverName() !== 'sqlite') {
            $this->markTestSkipped('Portable view-safety contract currently uses SQLite syntax.');
        }

        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        DB::statement('create view post_titles as select id, title from posts');

        try {
            $native = static fn() => Author::withoutCache()
                ->addSelect([
                    'post_count' => DB::table('post_titles')->selectRaw('count(*)'),
                ])
                ->whereKey($author->id)
                ->first();

            $cached = static fn() => Author::query()
                ->addSelect([
                    'post_count' => DB::table('post_titles')->selectRaw('count(*)'),
                ])
                ->dependsOn(['posts'])
                ->whereKey($author->id)
                ->first();

            $this->contract(
                $cached,
                $native,
                mutate: static fn() => Post::create([
                    'title' => 'P2',
                    'author_id' => $author->id,
                ]),
            );
        } finally {
            DB::statement('drop view if exists post_titles');
        }
    }

    /** withoutCache() is the opt-out; NormCache will not prove the function stable. */
    #[DataProvider('unknownFunctionExpressions')]
    public function test_unknown_database_function_is_cached_rather_than_bypassed(
        string $expression,
    ): void {
        if (DB::connection()->getDriverName() !== 'sqlite') {
            $this->markTestSkipped('The test function is registered through SQLite PDO.');
        }

        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        $database = (string) config('database.connections.testing.database');
        $reader = new \PDO("sqlite:{$database}");
        DB::connection()->getPdo()->sqliteCreateFunction(
            'normcache_post_count',
            static fn(): int => (int) $reader
                ->query('select count(*) from posts')
                ->fetchColumn(),
            0,
        );
        $query = static fn() => Author::query()
            ->selectRaw("{$expression} as post_count")
            ->whereKey($author->id)
            ->first();
        $native = static fn() => Author::withoutCache()
            ->selectRaw("{$expression} as post_count")
            ->whereKey($author->id)
            ->first();

        $this->contract($query, $native);
    }

    public static function unknownFunctionExpressions(): array
    {
        return [
            'bare' => ['normcache_post_count()'],
            'quoted identifier' => ['"normcache_post_count"()'],
        ];
    }

    /**
     * A comment in a raw fragment is DependencyAnalyzer's concern, not the
     * volatility scan's: it may hide a reference to another source.
     */
    public function test_a_commented_raw_projection_is_still_opaque(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $query = static fn() => Author::query()
            ->selectRaw('1/**/ as post_count')
            ->whereKey($author->id)
            ->first();
        $native = static fn() => Author::withoutCache()
            ->selectRaw('1/**/ as post_count')
            ->whereKey($author->id)
            ->first();

        $this->bypassContract($query, $native, reason: 'unidentifiable_dependency');
    }

    public function test_raw_join_expression_requires_declared_dependencies(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        Comment::create([
            'body' => 'Alice comment',
            'commentable_type' => Author::class,
            'commentable_id' => $alice->id,
        ]);

        $query = static fn(bool $native, bool $declared = false) => ($native ? Author::withoutCache() : Author::query())
            ->join(DB::raw('comments'), 'comments.commentable_id', '=', 'authors.id')
            ->when($declared, fn($query) => $query->dependsOn([Comment::class]))
            ->where('comments.commentable_type', Author::class)
            ->select('authors.*')
            ->orderBy('authors.id')
            ->get();

        $this->bypassContract(
            static fn() => $query(false),
            static fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            static fn() => $query(false, true),
            static fn() => $query(true),
            mutate: static fn() => Comment::create([
                'body' => 'Bob comment',
                'commentable_type' => Author::class,
                'commentable_id' => $bob->id,
            ]),
        );
    }

    public function test_raw_from_expression_requires_declared_dependencies(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $author->id]);
        $query = static fn(bool $native, bool $declared = false): int => ($native ? Author::withoutCache() : Author::query())
            ->fromRaw('posts as raw_posts')
            ->when($declared, fn($query) => $query->dependsOn([Post::class]))
            ->count();

        $this->bypassContract(
            static fn() => $query(false),
            static fn() => $query(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            static fn() => $query(false, true),
            static fn() => $query(true),
            mutate: static fn() => Post::create([
                'title' => 'P2',
                'author_id' => $author->id,
            ]),
        );
    }

    public function test_opaque_derived_predicate_requires_declared_dependencies(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create([
            'title' => 'Published',
            'author_id' => $author->id,
            'published' => true,
        ]);
        $build = static function (bool $native, bool $declared = false) {
            $publishedAuthors = DB::query()
                ->fromSub(
                    Post::query()
                        ->select('author_id')
                        ->where('published', true),
                    'published_posts',
                )
                ->select('published_posts.author_id');

            return ($native ? Author::withoutCache() : Author::query())
                ->whereIn('authors.id', $publishedAuthors)
                ->when($declared, fn($query) => $query->dependsOn(['posts']))
                ->orderBy('authors.id')
                ->get();
        };

        $this->bypassContract(
            static fn() => $build(false),
            static fn() => $build(true),
            reason: 'unidentifiable_dependency',
        );
        $this->contract(
            static fn() => $build(false, true),
            static fn() => $build(true),
        );
    }
}
