<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

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

        $cached = fn() => $build()->dependsOnTables(['comments'])->get();
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
            ->dependsOnTables(['posts'])
            ->select(['id', 'title']);

        $this->assertCount(1, $build()->get());
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(1, $build()->get());
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

        $declared = fn() => $base()->dependsOnTables(['comments'])->get();
        $declared();
        DB::flushQueryLog();
        DB::enableQueryLog();
        $declared();
        DB::disableQueryLog();
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_direct_root_calculated_projection_uses_table_local_exact_caching(): void
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
}
