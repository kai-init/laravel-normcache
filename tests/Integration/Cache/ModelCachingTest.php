<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedAuthor;
use NormCache\Tests\TestCase;
use stdClass;

class ModelCachingTest extends TestCase
{
    public function test_querying_models_populates_cache(): void
    {
        Author::create(['name' => 'Alice']);

        Author::all();

        $this->assertNotEmpty($this->redisKeys('test:*'));
    }

    public function test_get_returns_same_results_as_uncached_baseline(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $cached = Author::all()->pluck('name')->sort()->values();
        $live = UncachedAuthor::all()->pluck('name')->sort()->values();

        $this->assertEquals($live, $cached);
    }

    public function test_cache_disabled_globally_skips_caching(): void
    {
        $this->app['config']->set('normcache.enabled', false);

        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertEmpty($this->redisKeys('test:query:*'));
    }

    public function test_flush_command_without_model_flushes_all_keys(): void
    {
        Author::create(['name' => 'Alice']);
        Author::all();

        $this->assertNotEmpty($this->redisKeys('test:*'));

        $this->artisan('normcache:flush')->assertSuccessful();

        $this->assertEmpty($this->redisKeys('test:*'));
    }

    public function test_flush_command_with_model_flushes_only_that_model(): void
    {
        $author = Author::create(['name' => 'Alice']);
        Author::all();
        Post::create(['title' => 'Hello', 'author_id' => $author->id]);
        Post::all();

        $this->artisan('normcache:flush', ['--model' => Author::class])->assertSuccessful();

        $default = DB::getDefaultConnection();

        $this->assertEmpty($this->redisKeys('test:model:{' . $default . ':authors}:*'));
        $this->assertNotEmpty($this->redisKeys('test:model:{' . $default . ':posts}:*'));
    }

    public function test_flush_command_rejects_nonexistent_class(): void
    {
        $this->artisan('normcache:flush', ['--model' => 'App\\Models\\DoesNotExist'])
            ->assertFailed();
    }

    public function test_flush_command_rejects_class_without_cacheable_trait(): void
    {
        $this->artisan('normcache:flush', ['--model' => stdClass::class])
            ->assertFailed();
    }
}
