<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use ReflectionMethod;

class PivotCacheTest extends TestCase
{
    public function test_belongs_to_many_attach_invalidates_both_sides(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);

        $authorVersionBefore = NormCache::currentVersion(Author::class);
        $tagVersionBefore = NormCache::currentVersion(Tag::class);

        $author->tags()->attach($tag->id);

        $this->assertGreaterThan($authorVersionBefore, NormCache::currentVersion(Author::class));
        $this->assertGreaterThan($tagVersionBefore, NormCache::currentVersion(Tag::class));
    }

    public function test_belongs_to_many_detach_invalidates_both_sides(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $authorVersionBefore = NormCache::currentVersion(Author::class);
        $tagVersionBefore = NormCache::currentVersion(Tag::class);

        $author->tags()->detach($tag->id);

        $this->assertGreaterThan($authorVersionBefore, NormCache::currentVersion(Author::class));
        $this->assertGreaterThan($tagVersionBefore, NormCache::currentVersion(Tag::class));
    }

    public function test_belongs_to_many_sync_invalidates_once(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);

        $authorVersionBefore = NormCache::currentVersion(Author::class);

        $author->tags()->sync([$tag1->id, $tag2->id]);

        $this->assertSame(
            $authorVersionBefore + 1,
            NormCache::currentVersion(Author::class)
        );
    }

    public function test_belongs_to_many_update_existing_pivot_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $authorVersionBefore = NormCache::currentVersion(Author::class);

        $author->tags()->updateExistingPivot($tag->id, ['notes' => 'updated']);

        $this->assertGreaterThan($authorVersionBefore, NormCache::currentVersion(Author::class));
    }

    public function test_morph_to_many_attach_invalidates_both_sides(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);

        $postVersionBefore = NormCache::currentVersion(Post::class);
        $tagVersionBefore = NormCache::currentVersion(Tag::class);

        $post->tags()->attach($tag->id);

        $this->assertGreaterThan($postVersionBefore, NormCache::currentVersion(Post::class));
        $this->assertGreaterThan($tagVersionBefore, NormCache::currentVersion(Tag::class));
    }

    public function test_eager_load_populates_pivot_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        $this->assertNotEmpty($this->redisKeys('test:pivot:*'));
    }

    public function test_belongs_to_many_warm_hit_zero_sql(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        DB::enableQueryLog();
        Author::with('tags')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
    }

    public function test_empty_relationship_is_cached(): void
    {
        Author::create(['name' => 'Alice']);

        Author::with('tags')->get();

        $this->assertNotEmpty($this->redisKeys('test:pivot:*'));

        $authors = Author::with('tags')->get();

        $this->assertCount(0, $authors->first()->tags);
    }

    public function test_morph_to_many_warm_hit_zero_sql(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);
        $post->tags()->attach($tag->id);

        Post::with('tags')->get();

        DB::enableQueryLog();
        Post::with('tags')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
    }

    public function test_pivot_attributes_are_preserved_on_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();
        $authors = Author::with('tags')->get();

        $pivot = $authors->first()->tags->first()->pivot;
        $this->assertSame($author->id, $pivot->author_id);
        $this->assertSame($tag->id, $pivot->tag_id);
    }

    public function test_pivot_fk_columns_are_not_stored_in_related_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        $cached = $this->modelCacheEntry(Tag::class, $tag->id);

        $this->assertIsArray($cached);
        $this->assertArrayNotHasKey('pivot_author_id', $cached);
        $this->assertArrayNotHasKey('pivot_tag_id', $cached);
    }

    public function test_pivot_extra_columns_are_not_stored_in_related_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id, ['notes' => 'important']);

        Author::with(['tags' => fn($q) => $q->withPivot('notes')])->get();

        $cached = $this->modelCacheEntry(Tag::class, $tag->id);

        $this->assertIsArray($cached);
        $this->assertArrayNotHasKey('pivot_notes', $cached);
    }

    public function test_tag_served_from_model_cache_has_no_pivot_attributes(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        $fetched = Tag::find($tag->id);

        $this->assertNotNull($fetched);
        $this->assertArrayNotHasKey('pivot_author_id', $fetched->getRawOriginal());
        $this->assertArrayNotHasKey('pivot_tag_id', $fetched->getRawOriginal());
    }

    public function test_stale_pivot_cache_entry_can_remain_after_parent_version_bump(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with('tags')->get();

        $staleAuthorVersion = NormCache::currentVersion(Author::class);
        $staleTagVersion = NormCache::currentVersion(Tag::class);

        $author->update(['name' => 'Alice Updated']);

        $this->assertGreaterThan($staleAuthorVersion, NormCache::currentVersion(Author::class));

        $staleKeys = $this->redisKeys("test:pivot:*:v{$staleAuthorVersion}:v{$staleTagVersion}:*");

        $this->assertNotEmpty($staleKeys);
        $this->assertSame(['Fiction'], Author::with('tags')->first()->tags->pluck('name')->all());
    }

    public function test_pivot_warm_hit_runs_after_query_callbacks(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $count = 0;

        Author::with(['tags' => function ($query) use (&$count) {
            $query->afterQuery(function () use (&$count) {
                $count++;
            });
        }])->get();

        Author::with(['tags' => function ($query) use (&$count) {
            $query->afterQuery(function () use (&$count) {
                $count++;
            });
        }])->get();

        $this->assertSame(2, $count);
    }

    public function test_pivot_warm_hit_replays_nested_eager_loads(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag->posts()->attach($post->id);
        $author->tags()->attach($tag->id);

        Author::with('tags.posts')->get();
        $authors = Author::with('tags.posts')->get();

        $fetchedTag = $authors->first()->tags->first();

        $this->assertTrue($fetchedTag->relationLoaded('posts'));
        $this->assertSame([$post->id], $fetchedTag->posts->modelKeys());
    }

    public function test_pivot_cache_constrained_eager_load_does_not_collide_with_unconstrained(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);
        $author->tags()->attach($tag1->id, ['notes' => 'special']);
        $author->tags()->attach($tag2->id);

        Author::with(['tags' => fn($q) => $q->wherePivot('notes', 'special')])->get();

        $tags = Author::with('tags')->get()->first()->tags;

        $this->assertCount(2, $tags);
    }

    public function test_pivot_cache_ordered_eager_loads_do_not_collide(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $fiction = Tag::create(['name' => 'Fiction']);
        $drama = Tag::create(['name' => 'Drama']);
        $author->tags()->attach([$fiction->id, $drama->id]);

        Author::with(['tags' => fn($q) => $q->orderBy('tags.name')])->get();

        $tags = Author::with(['tags' => fn($q) => $q->orderByDesc('tags.name')])
            ->get()
            ->first()
            ->tags
            ->pluck('name')
            ->values()
            ->all();

        $this->assertSame(['Fiction', 'Drama'], $tags);
    }

    public function test_constraint_hash_changes_when_join_distinct_or_lock_added(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $base = $this->callConstraintHash($author->tags());

        $distinct = $author->tags();
        $distinct->getQuery()->distinct();
        $this->assertNotSame($base, $this->callConstraintHash($distinct));

        $lock = $author->tags();
        $lock->getQuery()->lockForUpdate();
        $this->assertNotSame($base, $this->callConstraintHash($lock));

        $join = $author->tags();
        $join->getQuery()->join('author_tag as at2', 'at2.tag_id', '=', 'tags.id');
        $this->assertNotSame($base, $this->callConstraintHash($join));
    }

    public function test_constraint_hash_distinguishes_nested_closure_wheres(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $fiction = $author->tags();
        $fiction->getQuery()->where(function ($query) {
            $query->where('tags.name', 'Fiction');
        });

        $drama = $author->tags();
        $drama->getQuery()->where(function ($query) {
            $query->where('tags.name', 'Drama');
        });

        $this->assertNotSame(
            $this->callConstraintHash($fiction),
            $this->callConstraintHash($drama)
        );
    }

    private function callConstraintHash(object $relation): string
    {
        $method = new ReflectionMethod($relation, 'currentConstraintHash');

        return $method->invoke($relation, ['*']);
    }
}
