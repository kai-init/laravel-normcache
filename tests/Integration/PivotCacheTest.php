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

    public function test_belongs_to_many_warm_hit_returns_correct_tags(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);
        $author->tags()->attach([$tag1->id, $tag2->id]);

        Author::with('tags')->get();
        $authors = Author::with('tags')->get();

        $this->assertCount(2, $authors->first()->tags);
        $this->assertEqualsCanonicalizing(
            ['Fiction', 'Drama'],
            $authors->first()->tags->pluck('name')->all()
        );
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

    public function test_morph_to_many_warm_hit_returns_correct_tags(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $tag = Tag::create(['name' => 'Fiction']);
        $post->tags()->attach($tag->id);

        Post::with('tags')->get();
        $posts = Post::with('tags')->get();

        $this->assertCount(1, $posts->first()->tags);
        $this->assertSame('Fiction', $posts->first()->tags->first()->name);
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

    public function test_attach_after_warm_hit_returns_updated_tags(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);
        $author->tags()->attach($tag1->id);

        Author::with('tags')->get();

        $author->tags()->attach($tag2->id);

        $authors = Author::with('tags')->get();

        $this->assertCount(2, $authors->first()->tags);
    }

    public function test_multiple_parents_warm_hit(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $bob = Author::create(['name' => 'Bob']);
        $fiction = Tag::create(['name' => 'Fiction']);
        $drama = Tag::create(['name' => 'Drama']);
        $alice->tags()->attach($fiction->id);
        $bob->tags()->attach($drama->id);

        Author::with('tags')->get();
        $authors = Author::with('tags')->get()->keyBy('name');

        $this->assertSame('Fiction', $authors['Alice']->tags->first()->name);
        $this->assertSame('Drama', $authors['Bob']->tags->first()->name);
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

    public function test_pivot_cache_two_different_constraints_stay_separate(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag1 = Tag::create(['name' => 'Fiction']);
        $tag2 = Tag::create(['name' => 'Drama']);
        $author->tags()->attach($tag1->id, ['notes' => 'a']);
        $author->tags()->attach($tag2->id, ['notes' => 'b']);

        Author::with(['tags' => fn($q) => $q->wherePivot('notes', 'a')])->get();
        Author::with(['tags' => fn($q) => $q->wherePivot('notes', 'b')])->get();

        $tagsA = Author::with(['tags' => fn($q) => $q->wherePivot('notes', 'a')])->get()->first()->tags;
        $tagsB = Author::with(['tags' => fn($q) => $q->wherePivot('notes', 'b')])->get()->first()->tags;

        $this->assertCount(1, $tagsA);
        $this->assertSame('Fiction', $tagsA->first()->name);

        $this->assertCount(1, $tagsB);
        $this->assertSame('Drama', $tagsB->first()->name);
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
