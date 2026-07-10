<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Enums\CacheOperation;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\CatalogTag;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\ReportingAuthor;
use NormCache\Tests\Fixtures\Models\ReportingCountry;
use NormCache\Tests\Fixtures\Models\SpacedAuthor;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CachePlanContext;

class SpaceResolutionTest extends TestCase
{
    public function test_space_method_sets_and_reads_back_the_space(): void
    {
        $builder = Post::query()->space('content');

        $this->assertSame('content', $builder->getSpace());
    }

    public function test_space_is_null_by_default(): void
    {
        $this->assertNull(Post::query()->getSpace());
    }

    public function test_cross_space_dependency_bypasses(): void
    {
        $plan = SpacedPost::query()
            ->dependsOn([Author::class])
            ->cachePlan(SpacedPost::query()->toBase(), CachePlanContext::models());

        $this->assertFalse($plan->isCacheable(), 'SpacedPost(content) depending on Author(default) must bypass');
        $this->assertSame(CacheOperation::Models, $plan->operation);
        $this->assertTrue($plan->hasBypassReason('dependency'));
    }

    public function test_same_space_dependency_still_caches(): void
    {
        // No declared spaces on Post/Author → both in default → no cross-space bypass.
        $plan = Post::query()
            ->dependsOn([Author::class])
            ->cachePlan(Post::query()->toBase(), CachePlanContext::models());

        $this->assertTrue($plan->isCacheable(), 'default-space deps must not be downgraded');
    }

    public function test_cacheable_plan_carries_the_resolved_space(): void
    {
        $plan = SpacedPost::query()->cachePlan(
            SpacedPost::query()->toBase(),
            CachePlanContext::models(),
        );

        $this->assertTrue($plan->isCacheable());
        $this->assertSame('content', $plan->space?->name);
    }

    public function test_default_model_plan_carries_default_space(): void
    {
        $plan = Post::query()->cachePlan(
            Post::query()->toBase(),
            CachePlanContext::models(),
        );

        $this->assertSame('default', $plan->space?->name);
    }

    public function test_spaced_model_query_writes_keys_under_its_space_tag(): void
    {
        Post::create(['title' => 'Hello', 'author_id' => 1]);

        SpacedPost::query()->get();

        $store = $this->cacheManager()->store();

        $this->assertNotEmpty(
            $store->scanPattern('{nc:content}:*'),
            'SpacedPost (content) must write keys under the {nc:content} hash tag',
        );
    }

    public function test_spaced_model_write_invalidates_its_space_cache(): void
    {
        SpacedPost::create(['title' => 'First', 'author_id' => 1]);

        $this->assertSame('First', SpacedPost::query()->get()->first()->title);

        $post = SpacedPost::query()->get()->first();
        $post->update(['title' => 'Second']);

        $this->assertSame(
            'Second',
            SpacedPost::query()->get()->first()->title,
            'content-space cache must invalidate when a content model is written',
        );
    }

    public function test_current_version_reads_spaced_model_home_space(): void
    {
        $before = $this->cacheManager()->currentVersion(SpacedPost::class);

        $this->cacheManager()->forceFlushModel(SpacedPost::class);

        $this->assertGreaterThan($before, $this->cacheManager()->currentVersion(SpacedPost::class));
    }

    public function test_simple_through_relation_caches_under_related_space(): void
    {
        $country = ReportingCountry::create(['name' => 'Australia']);
        $author = SpacedAuthor::create(['name' => 'Alice', 'country_id' => $country->id]);
        SpacedPost::create(['title' => 'Hello', 'author_id' => $author->id]);

        $this->assertSame(['Hello'], $country->spacedPosts()->get()->pluck('title')->all());

        $store = $this->cacheManager()->store();
        $this->assertNotEmpty($store->scanPattern('{nc:content}:test:through:*'));
        $this->assertEmpty($store->scanPattern('{nc}:test:through:*'));
    }

    public function test_non_simple_through_relation_bypasses_when_through_model_is_in_another_space(): void
    {
        $country = ReportingCountry::create(['name' => 'Australia']);
        $author = ReportingAuthor::create(['name' => 'Alice', 'country_id' => $country->id]);
        SpacedPost::create(['title' => 'Hello', 'author_id' => $author->id]);

        $posts = $country->crossSpacePosts()
            ->dependsOn([SpacedPost::class])
            ->get();

        $this->assertSame(['Hello'], $posts->pluck('title')->all());
        $this->assertEmpty($this->cacheManager()->store()->scanPattern('{nc:content}:test:through:*'));
    }

    public function test_spaced_pivot_relation_invalidates_when_pivot_table_changes(): void
    {
        $post = SpacedPost::create(['title' => 'First', 'author_id' => 1]);
        $firstTag = CatalogTag::create(['name' => 'First']);
        $secondTag = CatalogTag::create(['name' => 'Second']);
        $post->catalogTags()->attach($firstTag->id);

        $first = SpacedPost::query()->with('catalogTags')->get()->first()->catalogTags->pluck('name')->all();
        $this->assertSame(['First'], $first);
        $this->assertNotEmpty($this->cacheManager()->store()->scanPattern('{nc:catalog}:test:pivot:*'));

        $post->catalogTags()->attach($secondTag->id);

        $second = SpacedPost::query()->with('catalogTags')->get()->first()->catalogTags->pluck('name')->sort()->values()->all();
        $this->assertSame(['First', 'Second'], $second);
    }

    public function test_flush_all_removes_spaced_keys(): void
    {
        SpacedPost::create(['title' => 'First', 'author_id' => 1]);

        SpacedPost::query()->get();

        $store = $this->cacheManager()->store();
        $this->assertNotEmpty($store->scanPattern('{nc:content}:test:*'));

        $this->cacheManager()->flushAll();

        $this->assertEmpty($store->scanPattern('{nc:content}:test:*'));
    }

    public function test_flush_tag_removes_spaced_tagged_keys(): void
    {
        SpacedPost::create(['title' => 'First', 'author_id' => 1]);

        SpacedPost::query()->tag('homepage')->get();

        $store = $this->cacheManager()->store();
        $this->assertNotEmpty($store->scanPattern('{nc:content}:test:query:*:homepage:*'));

        $this->cacheManager()->flushTag(SpacedPost::class, 'homepage');

        $this->assertEmpty($store->scanPattern('{nc:content}:test:query:*:homepage:*'));
    }

    public function test_flush_tag_across_models_removes_spaced_tagged_keys(): void
    {
        SpacedPost::create(['title' => 'First', 'author_id' => 1]);

        SpacedPost::query()->tag('deploy')->get();

        $store = $this->cacheManager()->store();
        $this->assertNotEmpty($store->scanPattern('{nc:content}:test:query:*:deploy:*'));

        $this->cacheManager()->flushTagAcrossModels('deploy');

        $this->assertEmpty($store->scanPattern('{nc:content}:test:query:*:deploy:*'));
    }

    public function test_explain_shows_the_resolved_space(): void
    {
        $this->assertStringContainsString('[space: content]', SpacedPost::query()->explain());
        $this->assertStringNotContainsString('[space:', Post::query()->explain());
    }

    public function test_explain_reports_cross_space_bypass(): void
    {
        $explain = SpacedPost::query()->dependsOn([Author::class])->explain();

        $this->assertStringContainsString('cross-space', $explain);
    }

    public function test_co_located_relation_eager_load_caches_under_the_space_tag(): void
    {
        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'Hi', 'author_id' => $author->id]);

        $post = SpacedPost::query()->with('spacedAuthor')->get()->first();

        $this->assertSame('Ann', $post->spacedAuthor->name);

        $store = $this->cacheManager()->store();
        $authorKeys = $store->scanPattern('{nc:content}:test:model:*authors*');

        $this->assertNotEmpty(
            $authorKeys,
            'co-located belongsTo (content) must cache the related model under {nc:content}',
        );
    }

    public function test_pivot_invalidation_only_bumps_spaces_of_involved_models(): void
    {
        $registry = app(CacheSpaceRegistry::class);
        $registry->space('content');
        $registry->space('catalog');
        $registry->space('reporting');

        $store = $this->cacheManager()->store();

        $post = SpacedPost::create(['title' => 'Draft', 'author_id' => 1]);
        $tag = CatalogTag::create(['name' => 'PHP']);
        $post->catalogTags()->attach($tag->id);

        $this->assertNotEmpty($store->scanPattern('{nc:content}:test:ver:*taggables*'));
        $this->assertNotEmpty($store->scanPattern('{nc:catalog}:test:ver:*taggables*'));
        $this->assertEmpty($store->scanPattern('{nc:reporting}:test:ver:*taggables*'));
    }
}
