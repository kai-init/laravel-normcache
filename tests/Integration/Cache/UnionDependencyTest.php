<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheHit;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

final class UnionDependencyTest extends TestCase
{
    public function test_same_table_union_uses_result_storage_and_invalidates_on_write(): void
    {
        $first = Author::create(['name' => 'First']);
        $second = Author::create(['name' => 'Second']);
        $read = fn(): array => DB::table('authors')
            ->select(['id', 'name'])
            ->where('id', $first->getKey())
            ->union(
                DB::table('authors')
                    ->select(['id', 'name'])
                    ->where('id', $second->getKey()),
            )
            ->orderBy('id')
            ->pluck('name')
            ->all();

        $this->assertSame(['First', 'Second'], $read());
        Event::fake([QueryCacheHit::class]);
        $this->assertSame(['First', 'Second'], $read());
        Event::assertDispatched(
            QueryCacheHit::class,
            fn(QueryCacheHit $event): bool => $event->route === 'result',
        );

        Author::whereKey($second->getKey())->update(['name' => 'Changed']);

        $this->assertSame(['First', 'Changed'], $read());
    }

    public function test_cross_table_union_uses_query_group_and_tracks_both_tables(): void
    {
        Author::create(['name' => 'Author']);
        Tag::create(['name' => 'Tag']);
        $read = fn(): array => DB::table('authors')
            ->select('name')
            ->union(DB::table('tags')->select('name'))
            ->orderBy('name')
            ->pluck('name')
            ->all();

        $this->assertSame(['Author', 'Tag'], $read());
        Event::fake([QueryCacheHit::class]);
        $this->assertSame(['Author', 'Tag'], $read());
        Event::assertDispatched(
            QueryCacheHit::class,
            fn(QueryCacheHit $event): bool => $event->route === 'query_group',
        );

        Tag::create(['name' => 'Second Tag']);

        $this->assertSame(['Author', 'Second Tag', 'Tag'], $read());
    }

    public function test_nested_union_dependencies_invalidate_the_outer_query(): void
    {
        $author = Author::create(['name' => 'Author']);
        Post::create(['title' => 'Post', 'author_id' => $author->getKey()]);
        $read = function (): array {
            $authorIds = DB::table('authors')
                ->select('id')
                ->where('name', 'Missing')
                ->union(
                    DB::table('tags')
                        ->select('id')
                        ->where('name', 'Included'),
                );

            return DB::table('posts')
                ->whereIn('author_id', $authorIds)
                ->orderBy('id')
                ->pluck('title')
                ->all();
        };

        $this->assertSame([], $read());
        $this->assertSame([], $read());

        Tag::create(['id' => $author->getKey(), 'name' => 'Included']);

        $this->assertSame(['Post'], $read());
    }
}
