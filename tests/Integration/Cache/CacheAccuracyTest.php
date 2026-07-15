<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use ReflectionProperty;

/**
 * Behavioral tests: verifies that column aliasing, subclass hydration, join artifacts from
 * through/pivot relations, and mixed-PK bulk updates never serve outdated or mismatched data.
 */
class CacheAccuracyTest extends TestCase
{
    public function test_assoc_array_select_does_not_act_like_an_alias(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Author::find($author->id);

        $cached = Author::whereKey($author->id)
            ->select(['display_name' => 'name'])
            ->first();

        $this->assertArrayHasKey('name', $cached->getAttributes());
        $this->assertSame('Alice', $cached->name);
        $this->assertArrayNotHasKey('display_name', $cached->getAttributes());
    }

    public function test_expression_alias_columns_bypass_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Event::fake([ModelCacheHit::class, ModelCacheMiss::class, QueryCacheHit::class, QueryCacheMiss::class]);

        $author = Author::select(DB::raw('count(*) as total'))->first();

        $this->assertSame(1, (int) $author->total);
        Event::assertNotDispatched(ModelCacheHit::class);
        Event::assertNotDispatched(ModelCacheMiss::class);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_json_selector_alias_columns_bypass_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create([
            'title' => 'Hello',
            'author_id' => $author->id,
            'metadata' => ['section' => 'tech'],
        ]);

        Event::fake([ModelCacheHit::class, ModelCacheMiss::class, QueryCacheHit::class, QueryCacheMiss::class]);

        $post = Post::select('metadata->section as section_name')->first();

        $this->assertArrayHasKey('section_name', $post->getAttributes());
        Event::assertNotDispatched(ModelCacheHit::class);
        Event::assertNotDispatched(ModelCacheMiss::class);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_json_selector_columns_without_alias_bypass_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Post::create([
            'title' => 'Hello',
            'author_id' => $author->id,
            'metadata' => ['section' => 'tech'],
        ]);

        Event::fake([ModelCacheHit::class, ModelCacheMiss::class, QueryCacheHit::class, QueryCacheMiss::class]);

        Post::select('metadata->section')->first();

        Event::assertNotDispatched(ModelCacheHit::class);
        Event::assertNotDispatched(ModelCacheMiss::class);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
    }

    public function test_pivot_eager_load_with_selected_columns_does_not_poison_model_cache(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Author::with(['tags' => fn($query) => $query->select('tags.id')])->get();

        $cachedTag = Tag::find($tag->id);

        $this->assertSame('Fiction', $cachedTag->name);
    }

    public function test_pivot_selected_columns_are_preserved_on_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Tag::find($tag->id); // populate full model cache before the constrained eager load runs

        Author::with(['tags' => fn($query) => $query->select('tags.id')])->get();
        $warm = Author::with(['tags' => fn($query) => $query->select('tags.id')])->get();

        $attributes = $warm->first()->tags->first()->getAttributes();

        $this->assertArrayHasKey('id', $attributes);
        $this->assertArrayNotHasKey('name', $attributes);
    }

    public function test_through_relation_selected_columns_are_preserved_on_warm_hit(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = $author->posts()->create(['title' => 'Hello']);

        $post::find($post->id); // populate full model cache before the constrained eager load runs

        $country->posts()->select('posts.id')->get();
        $warm = $country->posts()->select('posts.id')->get();

        $attributes = $warm->first()->getAttributes();

        $this->assertArrayHasKey('id', $attributes);
        $this->assertArrayNotHasKey('title', $attributes);
    }

    public function test_through_relation_with_selected_columns_does_not_poison_model_cache(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = $author->posts()->create(['title' => 'Hello']);

        $country->posts()->select('posts.id')->get();

        $cachedPost = $post::find($post->id);

        $this->assertSame('Hello', $cachedPost->title);
    }

    public function test_subclass_hydrates_with_own_class_not_parent_data(): void
    {
        Author::create(['id' => 1, 'name' => 'Alice']);
        Author::find(1);

        $admin = AdminAuthor::find(1);

        $this->assertInstanceOf(AdminAuthor::class, $admin);
        $this->assertSame('ALICE', $admin->display_name);
    }

    public function test_pivot_relation_with_join_does_not_poison_unconstrained_warm_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $tag = Tag::create(['name' => 'Fiction']);
        $author->tags()->attach($tag->id);

        Post::create(['title' => 'First', 'author_id' => $author->id]);
        Post::create(['title' => 'Second', 'author_id' => $author->id]);

        Author::with([
            'tags' => fn($query) => $query->join('posts', 'posts.author_id', '=', 'author_tag.author_id'),
        ])->get();

        $warm = Author::with('tags')->get()->first()->tags;

        $this->assertSame([$tag->id], $warm->modelKeys());
    }

    public function test_mixed_pk_and_non_pk_bulk_update_does_not_leave_outdated_model_cache_entries(): void
    {
        $a1 = Author::create(['name' => 'Alice']);
        $a2 = Author::create(['name' => 'Bob']);

        Author::all();

        Author::where('id', $a1->id)
            ->orWhere('name', 'Bob')
            ->update(['name' => 'Updated']);

        $this->assertSame('Updated', Author::find($a1->id)->name);
        $this->assertSame('Updated', Author::find($a2->id)->name);
    }

    public function test_through_relation_with_trashed_scope_survives_related_model_cache_miss(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        $post->delete();

        $warm = $country->posts()->withTrashed()->get();
        $this->assertCount(1, $warm);
        $this->assertTrue($warm->first()->trashed());

        $this->evictModelCache(Post::class, $post->id);

        $cached = $country->posts()->withTrashed()->get();

        $this->assertCount(1, $cached);
        $this->assertTrue($cached->first()->trashed());
    }

    public function test_through_aggregate_cache_invalidates_when_intermediate_membership_changes(): void
    {
        $source = Country::create(['name' => 'Australia']);
        $target = Country::create(['name' => 'Canada']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $source->id]);
        $author->posts()->create(['title' => 'Hello']);

        $warm = Country::orderBy('id')->withCount('posts')->get()->keyBy('id');

        $this->assertSame(1, $warm[$source->id]->posts_count);
        $this->assertSame(0, $warm[$target->id]->posts_count);

        $author->update(['country_id' => $target->id]);

        $cached = Country::orderBy('id')->withCount('posts')->get()->keyBy('id');

        $this->assertSame(0, $cached[$source->id]->posts_count);
        $this->assertSame(1, $cached[$target->id]->posts_count);
    }

    public function test_belongs_to_aggregate_cache_invalidates_when_parent_foreign_key_changes(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice']);

        $warm = Author::orderBy('id')->withCount('country')->get()->first();

        $this->assertSame(0, $warm->country_count);

        $author->update(['country_id' => $country->id]);

        $cached = Author::orderBy('id')->withCount('country')->get()->first();

        $this->assertSame(1, $cached->country_count);
    }

    public function test_through_relation_selected_column_warm_hit_preserves_laravel_through_key_attribute(): void
    {
        $country = Country::create(['name' => 'Australia']);
        $author = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = $author->posts()->create(['title' => 'Hello']);

        $coldAttributes = $country->posts()->select('posts.id')->get()->first()->getAttributes();
        $warmAttributes = $country->posts()->select('posts.id')->get()->first()->getAttributes();

        $this->assertArrayHasKey('laravel_through_key', $coldAttributes);
        $this->assertSame($post->id, $warmAttributes['id']);
        $this->assertArrayHasKey('laravel_through_key', $warmAttributes);
        $this->assertSame($coldAttributes['laravel_through_key'], $warmAttributes['laravel_through_key']);
    }

    public function test_subclass_get_deleted_at_column_override_is_respected(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'Hello', 'author_id' => $author->id]);

        app('normcache')->hydrator()->getModels([$post->id], Post::class);

        $resolved = (new ReflectionProperty(CacheKeyBuilder::class, 'deletedAtColumns'))->getValue();
        $this->assertSame('deleted_at', $resolved[Post::class] ?? null);

        AltDeletedAtPost::resolveSoftDelete();
        $resolved = (new ReflectionProperty(CacheKeyBuilder::class, 'deletedAtColumns'))->getValue();

        $this->assertSame('archived_at', $resolved[AltDeletedAtPost::class] ?? null);
    }

    public function test_same_table_models_on_different_connections_do_not_share_cached_data(): void
    {
        config()->set('database.connections.secondary_testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);

        Schema::connection('secondary_testing')->create('authors', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->foreignId('country_id')->nullable();
            $table->timestamps();
        });

        Author::create(['id' => 1, 'name' => 'Primary Alice']);
        SecondaryConnectionAuthor::create(['id' => 1, 'name' => 'Secondary Alice']);

        Author::find(1);
        $secondary = SecondaryConnectionAuthor::find(1);

        $this->assertSame('Secondary Alice', $secondary->name);
        $this->assertSame(DB::getDefaultConnection() . ':authors', app('normcache')->keys()->classKey(Author::class));
        $this->assertSame('secondary_testing:authors', app('normcache')->keys()->classKey(SecondaryConnectionAuthor::class));
    }
}

class SecondaryConnectionAuthor extends Model
{
    use Cacheable;

    protected $connection = 'secondary_testing';

    protected $table = 'authors';

    protected $guarded = [];
}

class AdminAuthor extends Author
{
    protected $table = 'authors';

    public function getDisplayNameAttribute(): string
    {
        return strtoupper($this->name);
    }
}

class AltDeletedAtPost extends Post
{
    protected $table = 'posts';

    public function getDeletedAtColumn()
    {
        return 'archived_at';
    }

    public static function resolveSoftDelete(): void
    {
        $prototype = new self;
        $col = $prototype->getDeletedAtColumn();
        $prop = new ReflectionProperty(CacheKeyBuilder::class, 'deletedAtColumns');
        $current = $prop->getValue();
        $current[self::class] = $col;
        $prop->setValue(null, $current);
    }
}
