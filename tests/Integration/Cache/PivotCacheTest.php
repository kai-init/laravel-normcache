<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use ReflectionMethod;

/**
 * Behavioral tests: belongsToMany and morphToMany pivot caches are invalidated on
 * attach/detach/sync, served on warm hits with zero SQL, and preserve pivot attributes
 * without leaking FK columns into the related-model cache.
 */
class PivotCacheTest extends TestCase
{
    public function test_belongs_to_many_attach_invalidates_pivot_table(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);

        $versionBefore = NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag');

        $author->tags()->attach($tag->id);

        $this->assertGreaterThan($versionBefore, NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag'));
    }

    public function test_belongs_to_many_detach_invalidates_pivot_table(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $versionBefore = NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag');

        $author->tags()->detach($tag->id);

        $this->assertGreaterThan($versionBefore, NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag'));
    }

    public function test_belongs_to_many_sync_invalidates_once(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);

        $versionBefore = NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag');

        $author->tags()->sync([$tag1->id, $tag2->id]);

        $this->assertSame(
            $versionBefore + 1,
            NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag')
        );
    }

    public function test_belongs_to_many_update_existing_pivot_invalidates_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $versionBefore = NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag');

        $author->tags()->updateExistingPivot($tag->id, ['notes' => 'updated']);

        $this->assertGreaterThan($versionBefore, NormCache::currentTableVersion($author->getConnection()->getName(), 'author_tag'));
    }

    public function test_morph_to_many_attach_invalidates_pivot_table(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);

        $versionBefore = NormCache::currentTableVersion($post->getConnection()->getName(), 'taggables');

        $post->tags()->attach($tag->id);

        $this->assertGreaterThan($versionBefore, NormCache::currentTableVersion($post->getConnection()->getName(), 'taggables'));
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

    public function test_parent_version_bump_does_not_invalidate_pivot_membership_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        $author->tags()->get();

        $keyCountAfterWarm = count($this->redisKeys('test:pivot:*'));

        $author->update(['name' => 'Alice Updated']);

        DB::enableQueryLog();
        $tags = $author->tags()->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
        $this->assertSame($keyCountAfterWarm, count($this->redisKeys('test:pivot:*')));
        $this->assertSame(['Fiction'], $tags->pluck('name')->all());
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

    public function test_pivot_relation_with_extra_join_delegates_to_eloquent(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $author->tags()->attach($tag->id);

        $tags = $author->tags()
            ->join('posts', 'posts.author_id', '=', 'author_tag.author_id')
            ->get();

        $this->assertSame([$tag->id], $tags->modelKeys());
        $this->assertEmpty($this->redisKeys('test:pivot:*'));
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

    public function test_pivot_eager_load_different_batch_sizes_reuse_same_parent_cache(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        $fiction = Tag::create(['name' => 'Fiction']);
        $alice->tags()->attach($fiction->id);
        $bob->tags()->attach($fiction->id);

        // Warm pivot cache for batch [alice, bob]
        Author::with('tags')->get();

        // Second load for batch [alice] only should be a full cache hit (zero queries)
        DB::enableQueryLog();
        Author::with('tags')->whereKey($alice->id)->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries);
    }

    public function test_pivot_user_where_constraints_with_different_bindings_hash_differently(): void
    {
        $author = Author::create(['name' => 'Alice']);

        $phpRelation = $author->tags();
        $phpRelation->getQuery()->where('tags.name', 'php');

        $laravelRelation = $author->tags();
        $laravelRelation->getQuery()->where('tags.name', 'laravel');

        $this->assertNotSame(
            $this->callConstraintHash($phpRelation),
            $this->callConstraintHash($laravelRelation)
        );
    }

    public function test_pivot_constraint_hash_is_stable_across_eager_batch_sizes(): void
    {
        $author = Author::create(['name' => 'Alice']);

        // Simulate constraint hash as seen in batch [1, 2] vs batch [1]
        $relationBatchTwo = $author->tags();
        $relationBatchTwo->addEagerConstraints([
            Author::create(['name' => 'Bob']),
            $author,
        ]);

        $relationBatchOne = $author->tags();
        $relationBatchOne->addEagerConstraints([$author]);

        $this->assertSame(
            $this->callConstraintHash($relationBatchOne),
            $this->callConstraintHash($relationBatchTwo)
        );
    }

    private function callConstraintHash(object $relation): string
    {
        $method = new ReflectionMethod($relation, 'currentConstraintHash');

        return $method->invoke($relation, ['*']);
    }

    public function test_belongs_to_many_remains_fast_path(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'PHP']);
        $post->tags()->attach($tag);

        // Eager load belongsToMany - first call warms cache
        Post::with('tags')->get();

        // Verify total cache hit on second load
        DB::enableQueryLog();
        Post::with('tags')->get();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertEmpty($queries, 'Should hit total query cache');
    }
}
