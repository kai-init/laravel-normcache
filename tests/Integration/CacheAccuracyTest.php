<?php

namespace NormCache\Tests\Integration;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;

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
        $this->assertSame('authors', app('normcache')->classKey(Author::class));
        $this->assertSame('secondary_testing:authors', app('normcache')->classKey(SecondaryConnectionAuthor::class));
    }
}

class SecondaryConnectionAuthor extends Model
{
    use Cacheable;

    protected $connection = 'secondary_testing';

    protected $table = 'authors';

    protected $guarded = [];
}
