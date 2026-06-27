<?php

namespace NormCache\Tests\Integration\Infrastructure;

use Illuminate\Support\Facades\Redis;
use NormCache\CacheManager;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

/**
 * Verifies that cross-model cache operations produce correct results when a Redis Cluster
 * connection is used. All keys share the {nc} hash tag, preserving Lua atomicity.
 */
class ClusterModeTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->setClusterMode(true);
    }

    // dependsOn — multi-dependency normalized query stays normalized (no forced result cache)

    public function test_multi_dependency_normalized_query_stays_normalized_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);

        Author::query()->dependsOn([Post::class, Tag::class])->get();

        $this->assertNotEmpty($this->redisKeys('test:query:*'), 'Multi-dependency normalized queries should use query keys in cluster mode');
        $this->assertEmpty($this->redisKeys('test:result:*'), 'Multi-dependency normalized queries should not fall back to result cache');
    }

    public function test_depends_on_returns_correct_results_in_cluster_mode(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id, 'published' => true]);
        Author::create(['name' => 'Bob']);

        $native = Author::withoutCache()
            ->whereHas('posts', fn($q) => $q->where('published', true))
            ->orderBy('name')
            ->get()
            ->pluck('name')
            ->all();

        $cold = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->orderBy('name')
            ->get()
            ->pluck('name')
            ->all();

        $warm = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->orderBy('name')
            ->get()
            ->pluck('name')
            ->all();

        $this->assertSame($native, $cold);
        $this->assertSame($cold, $warm);
    }

    public function test_depends_on_invalidates_on_dep_version_bump_in_cluster_mode(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $post = Post::create(['title' => 'P1', 'author_id' => $alice->id, 'published' => true]);

        $first = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->get();

        $this->assertCount(1, $first);

        $post->update(['published' => false]);

        $second = Author::whereHas('posts', fn($q) => $q->where('published', true))
            ->dependsOn([Post::class])
            ->get();

        $this->assertCount(0, $second);
    }

    // Aggregate cache (withCount)

    public function test_with_count_returns_correct_results_in_cluster_mode(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);
        Post::create(['title' => 'P2', 'author_id' => $alice->id]);
        Author::create(['name' => 'Bob']);

        $native = Author::withoutCache()->withoutAggregateCache()->withCount('posts')->orderBy('name')->get()
            ->map(fn($a) => [$a->name, $a->posts_count])->all();

        $cold = Author::withCount('posts')->orderBy('name')->get()
            ->map(fn($a) => [$a->name, $a->posts_count])->all();

        $warm = Author::withCount('posts')->orderBy('name')->get()
            ->map(fn($a) => [$a->name, $a->posts_count])->all();

        $this->assertSame($native, $cold);
        $this->assertSame($cold, $warm);
    }

    public function test_with_count_invalidates_on_related_version_bump_in_cluster_mode(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        $first = Author::withCount('posts')->where('id', $alice->id)->get()->first();
        $this->assertSame(1, $first->posts_count);

        Post::create(['title' => 'P2', 'author_id' => $alice->id]);

        $second = Author::withCount('posts')->where('id', $alice->id)->get()->first();
        $this->assertSame(2, $second->posts_count);
    }

    // Pivot cache (BelongsToMany)

    public function test_belongs_to_many_returns_correct_results_in_cluster_mode(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $php = Tag::create(['name' => 'php']);
        $laravel = Tag::create(['name' => 'laravel']);
        $alice->tags()->attach([$php->id, $laravel->id]);

        $native = Author::withoutCache()->with('tags')->find($alice->id)->tags->pluck('name')->sort()->values()->all();

        $cold = Author::with('tags')->find($alice->id)->tags->pluck('name')->sort()->values()->all();
        $warm = Author::with('tags')->find($alice->id)->tags->pluck('name')->sort()->values()->all();

        $this->assertSame($native, $cold);
        $this->assertSame($cold, $warm);
    }

    public function test_belongs_to_many_invalidates_on_attach_in_cluster_mode(): void
    {
        $alice = Author::create(['name' => 'Alice']);
        $php = Tag::create(['name' => 'php']);

        $before = Author::with('tags')->find($alice->id)->tags->count();
        $this->assertSame(0, $before);

        $alice->tags()->attach($php->id);

        $after = Author::with('tags')->find($alice->id)->tags->count();
        $this->assertSame(1, $after);
    }

    // Through cache (HasManyThrough)

    public function test_has_many_through_returns_correct_results_in_cluster_mode(): void
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);
        Post::create(['title' => 'P2', 'author_id' => $alice->id]);

        $native = Country::withoutCache()->with('posts')->find($country->id)->posts->pluck('title')->sort()->values()->all();
        $cold = Country::with('posts')->find($country->id)->posts->pluck('title')->sort()->values()->all();
        $warm = Country::with('posts')->find($country->id)->posts->pluck('title')->sort()->values()->all();

        $this->assertSame($native, $cold);
        $this->assertSame($cold, $warm);
    }

    public function test_has_many_through_invalidates_on_post_write_in_cluster_mode(): void
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        Post::create(['title' => 'P1', 'author_id' => $alice->id]);

        $before = Country::with('posts')->find($country->id)->posts->count();
        $this->assertSame(1, $before);

        Post::create(['title' => 'P2', 'author_id' => $alice->id]);

        $after = Country::with('posts')->find($country->id)->posts->count();
        $this->assertSame(2, $after);
    }

    // Flush operations — verify keys are cleared across all nodes

    public function test_flush_all_clears_all_cache_keys_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        Author::get();
        Author::orderBy('name')->get();

        $this->assertNotEmpty($this->redisKeys('test:*'));

        $this->cacheManager()->flushAll();

        $this->assertEmpty($this->redisKeys('test:*'));
    }

    public function test_flush_tag_clears_only_tagged_keys_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);

        Author::tag('homepage')->get();
        Author::get();

        $taggedKeys = $this->redisKeys('test:query:*:homepage:*');
        $this->assertNotEmpty($taggedKeys, 'tagged query keys must exist before flush');

        $this->cacheManager()->flushTag(Author::class, 'homepage');

        $this->assertEmpty($this->redisKeys('test:query:*:homepage:*'), 'tagged keys must be gone after flushTag');
        $this->assertNotEmpty($this->redisKeys('test:query:*'), 'untagged query keys must survive flushTag');
    }

    public function test_flush_tag_across_models_clears_tagged_keys_for_all_models_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);
        Post::create(['title' => 'P1', 'author_id' => 1]);

        Author::tag('homepage')->get();
        Post::tag('homepage')->get();
        Author::get();

        $this->assertNotEmpty($this->redisKeys('test:query:*:homepage:*'), 'tagged keys must exist before flush');

        $this->cacheManager()->flushTagAcrossModels('homepage');

        $this->assertEmpty($this->redisKeys('test:query:*:homepage:*'), 'all tagged keys must be gone after flushTagAcrossModels');
        $this->assertNotEmpty($this->redisKeys('test:query:*'), 'untagged query keys must survive');
    }

    public function test_prefix_sensitive_scan_and_flush_operations_work_in_cluster_mode(): void
    {
        config()->set('database.redis.options.prefix', 'laravel:');
        Redis::purge('normcache-test');
        $this->app->forgetInstance(CacheManager::class);
        $this->app->forgetInstance('normcache');

        try {
            Author::create(['name' => 'Alice']);
            Post::create(['title' => 'P1', 'author_id' => 1]);

            Author::tag('homepage')->get();
            Post::tag('homepage')->get();
            Author::get();

            $this->assertNotEmpty($this->redisKeys('test:query:*'));
            foreach ($this->redisKeys('test:*') as $key) {
                $this->assertStringNotContainsString('laravel:', $key);
            }

            $this->cacheManager()->flushTag(Author::class, 'homepage');
            $this->assertEmpty($this->redisKeys('test:query:testing:authors:homepage:*'), 'author tagged keys must be gone');
            $this->assertNotEmpty($this->redisKeys('test:query:testing:posts:homepage:*'), 'post tagged keys must survive');

            $this->cacheManager()->flushTagAcrossModels('homepage');
            $this->assertEmpty($this->redisKeys('test:query:*:homepage:*'));
            $this->assertNotEmpty($this->redisKeys('test:query:*'));

            $removed = $this->cacheManager()->getStore()->flushByPatterns(['query:*']);
            $this->assertGreaterThan(0, $removed);
            $this->assertEmpty($this->redisKeys('test:query:*'));

            Author::get();
            $this->assertNotEmpty($this->redisKeys('test:*'));

            $this->cacheManager()->flushAll();
            $this->assertEmpty($this->redisKeys('test:*'));
        } finally {
            Redis::purge('normcache-test');
            config()->set('database.redis.options.prefix', '');
            $this->app->forgetInstance(CacheManager::class);
            $this->app->forgetInstance('normcache');
        }
    }

    // Standard single-model query cache (should be unaffected by cluster flag)

    public function test_single_model_query_cache_still_works_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);

        $native = Author::withoutCache()->orderBy('name')->get()->pluck('name')->all();
        $cold = Author::orderBy('name')->get()->pluck('name')->all();
        $warm = Author::orderBy('name')->get()->pluck('name')->all();

        $this->assertSame($native, $cold);
        $this->assertSame($cold, $warm);
    }

    public function test_single_model_invalidation_still_works_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);

        $before = Author::get()->count();
        $this->assertSame(1, $before);

        Author::create(['name' => 'Bob']);

        $after = Author::get()->count();
        $this->assertSame(2, $after);
    }

    // Version key TTL — incrementAndExpire cluster-safety (ensures hash tags route EVAL to owning node)

    public function test_version_key_has_ttl_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);

        $redis = Redis::connection('normcache-test');
        $classKey = $this->cacheManager()->classKey(Author::class);
        $verKey = '{nc}:test:ver:' . $classKey . ':';

        $ttl = $redis->ttl($verKey);

        $this->assertGreaterThan(0, $ttl, 'version key must carry a TTL in cluster mode');
    }

    // count() pagination total (getNamespacedCache)

    public function test_paginate_count_cache_works_in_cluster_mode(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::create(['name' => 'Carol']);

        $cold = Author::orderBy('name')->paginate(2);
        $warm = Author::orderBy('name')->paginate(2);

        $this->assertSame(3, $cold->total());
        $this->assertSame(3, $warm->total());
    }
}
