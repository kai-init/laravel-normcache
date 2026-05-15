<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use NormCache\CacheManager;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Fixtures\Models\UncachedAuthor;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use ReflectionProperty;

class CacheAccuracyTest extends TestCase
{
    public function test_selected_alias_columns_match_eloquent_shape_on_cache_miss(): void
    {
        Author::create(['name' => 'Alice']);

        $author = Author::select('name as display_name')->first();

        $this->assertArrayHasKey('display_name', $author->getAttributes());
        $this->assertSame('Alice', $author->display_name);
        $this->assertArrayNotHasKey('name', $author->getAttributes());
    }

    public function test_selected_alias_columns_match_eloquent_shape_on_model_cache_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Author::find($author->id);

        $cached = Author::whereKey($author->id)
            ->select('name as display_name')
            ->first();

        $this->assertArrayHasKey('display_name', $cached->getAttributes());
        $this->assertSame('Alice', $cached->display_name);
        $this->assertArrayNotHasKey('name', $cached->getAttributes());
    }

    public function test_qualified_alias_columns_match_eloquent_shape_on_model_cache_hit(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Author::find($author->id);

        $cached = Author::whereKey($author->id)
            ->select('authors.name as display_name')
            ->first();

        $this->assertArrayHasKey('display_name', $cached->getAttributes());
        $this->assertSame('Alice', $cached->display_name);
        $this->assertArrayNotHasKey('name', $cached->getAttributes());
    }

    public function test_alias_columns_match_laravel_as_casing_and_spacing(): void
    {
        $author = Author::create(['name' => 'Alice']);

        Author::find($author->id);

        $cached = Author::whereKey($author->id)
            ->select('authors.name   AS   display_name')
            ->first();

        $this->assertArrayHasKey('display_name', $cached->getAttributes());
        $this->assertSame('Alice', $cached->display_name);
        $this->assertArrayNotHasKey('name', $cached->getAttributes());
    }

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

        Tag::find($tag->id); // warm full model cache first

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

        $post::find($post->id); // warm full model cache first

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

    public function test_distinct_selected_column_query_matches_eloquent_results(): void
    {
        $australia = Country::create(['name' => 'Australia']);
        $canada = Country::create(['name' => 'Canada']);
        Author::create(['name' => 'Alice', 'country_id' => $australia->id]);
        Author::create(['name' => 'Bob', 'country_id' => $australia->id]);
        Author::create(['name' => 'Charlie', 'country_id' => $canada->id]);

        $cached = Author::select('country_id')
            ->distinct()
            ->get()
            ->pluck('country_id')
            ->values()
            ->all();

        $live = UncachedAuthor::select('country_id')
            ->distinct()
            ->get()
            ->pluck('country_id')
            ->values()
            ->all();

        $this->assertSame($live, $cached);
    }

    public function test_primary_key_where_in_matches_eloquent_ordering(): void
    {
        Author::create(['id' => 1, 'name' => 'A']);
        Author::create(['id' => 2, 'name' => 'B']);
        Author::create(['id' => 3, 'name' => 'C']);

        $live = UncachedAuthor::whereIn('id', [3, 1, 2])->get()->pluck('id')->all();
        $cached = Author::whereIn('id', [3, 1, 2])->get()->pluck('id')->all();

        $this->assertSame($live, $cached);
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

        app('normcache')->getModels([$post->id], Post::class);

        $resolved = (new ReflectionProperty(CacheManager::class, 'deletedAtColumns'))->getValue();
        $this->assertSame('deleted_at', $resolved[Post::class] ?? null);

        AltDeletedAtPost::resolveSoftDelete();
        $resolved = (new ReflectionProperty(CacheManager::class, 'deletedAtColumns'))->getValue();

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
        $this->assertSame(DB::getDefaultConnection() . ':authors', app('normcache')->classKey(Author::class));
        $this->assertSame('secondary_testing:authors', app('normcache')->classKey(SecondaryConnectionAuthor::class));
    }

    public function test_runtime_default_connection_swap_does_not_share_cached_data(): void
    {
        config()->set('database.connections.shard_b', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);

        Schema::connection('shard_b')->create('authors', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->foreignId('country_id')->nullable();
            $table->timestamps();
        });

        $original = DB::getDefaultConnection();

        try {
            Author::create(['id' => 1, 'name' => 'DefaultAlice']);
            Author::find(1);

            DB::setDefaultConnection('shard_b');
            DB::table('authors')->insert([
                'id' => 1,
                'name' => 'ShardBAlice',
                'created_at' => now(),
                'updated_at' => now(),
            ]);

            $shardB = Author::find(1);

            $this->assertSame('ShardBAlice', $shardB->name);
            $this->assertSame('shard_b:authors', app('normcache')->classKey(Author::class));
        } finally {
            DB::setDefaultConnection($original);
        }
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
        $prop = new ReflectionProperty(CacheManager::class, 'deletedAtColumns');
        $current = $prop->getValue();
        $current[self::class] = $col;
        $prop->setValue(null, $current);
    }
}
