<?php

namespace NormCache\Tests\Integration\Infrastructure;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\CatalogTag;
use NormCache\Tests\Fixtures\Models\MultiSpacePost;
use NormCache\Tests\Fixtures\Models\ReportingCountry;
use NormCache\Tests\Fixtures\Models\SpacedAuthor;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\Integration\Infrastructure\Concerns\InteractsWithClusterRedis;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\Group;
use RuntimeException;

#[Group('cluster')]
class ClusterSpacesTest extends TestCase
{
    use InteractsWithClusterRedis;

    protected function setUp(): void
    {
        parent::setUp();

        $this->requiresRedisCluster();
        $this->setClusterMode(true);
    }

    public function test_cluster_connection_is_real_cluster_backed(): void
    {
        $this->assertNotEmpty($this->clusterMasterNodes());
    }

    public function test_distinct_spaces_use_distinct_cluster_slots(): void
    {
        $default = $this->redisClusterSlot('nc');
        $content = $this->redisClusterSlot('nc:content');
        $catalog = $this->redisClusterSlot('nc:catalog');
        $reporting = $this->redisClusterSlot('nc:reporting');

        $this->assertNotSame($default, $content);
        $this->assertNotSame($default, $catalog);
        $this->assertNotSame($default, $reporting);
        $this->assertCount(4, array_unique([$default, $content, $catalog, $reporting]));
    }

    public function test_each_space_full_cache_lifecycle_runs_without_crossslot(): void
    {
        $this->assertNoCrossSlot(function () {
            $author = SpacedAuthor::create(['name' => 'Ann']);
            SpacedPost::create(['title' => 'Article', 'author_id' => $author->id]);
            CatalogTag::create(['name' => 'Widgets']);
            ReportingCountry::create(['name' => 'Atlantis']);

            $this->assertSame('Article', SpacedPost::query()->get()->first()->title);
            $this->assertSame('Article', SpacedPost::query()->get()->first()->title);

            $this->assertSame('Widgets', CatalogTag::query()->get()->first()->name);
            $this->assertSame('Widgets', CatalogTag::query()->get()->first()->name);

            $this->assertSame('Atlantis', ReportingCountry::query()->get()->first()->name);
            $this->assertSame('Atlantis', ReportingCountry::query()->get()->first()->name);
        });

        $this->assertAnyKeysForHashTag('nc:content', 'test:*');
        $this->assertAnyKeysForHashTag('nc:catalog', 'test:*');
        $this->assertAnyKeysForHashTag('nc:reporting', 'test:*');
    }

    public function test_explicit_space_query_with_allowed_dependencies_caches_in_that_space(): void
    {
        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'First', 'author_id' => $author->id]);

        $this->assertNoCrossSlot(function () {
            return SpacedPost::query()
                ->space('content')
                ->dependsOn([SpacedAuthor::class])
                ->get();
        });

        $this->assertSame(
            'First',
            SpacedPost::query()->space('content')->dependsOn([SpacedAuthor::class])->get()->first()->title,
        );
        $this->assertAnyKeysForHashTag('nc:content', 'test:query:*');
        $this->assertNoKeysForHashTag('nc', 'test:query:*posts*');

        $author->update(['name' => 'Annie']);

        $post = SpacedPost::query()->space('content')->with('spacedAuthor')->get()->first();
        $this->assertSame('Annie', $post->spacedAuthor->name);
    }

    public function test_cross_space_dependency_bypasses_before_redis(): void
    {
        config(['normcache.spaces.cross_space_behavior' => 'bypass']);

        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'First', 'author_id' => $author->id]);
        CatalogTag::create(['name' => 'Widgets']);

        $result = $this->assertNoCrossSlot(fn() => SpacedPost::query()
            ->space('content')
            ->dependsOn([CatalogTag::class])
            ->get());

        $this->assertSame('First', $result->first()->title);
        $this->assertNoKeysForHashTag('nc:content', 'test:query:*');
    }

    public function test_cross_space_dependency_throw_mode_fails_before_redis(): void
    {
        config(['normcache.spaces.cross_space_behavior' => 'throw']);

        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'First', 'author_id' => $author->id]);
        CatalogTag::create(['name' => 'Widgets']);

        try {
            SpacedPost::query()
                ->space('content')
                ->dependsOn([CatalogTag::class])
                ->get();

            $this->fail('Expected a cross-space planning exception.');
        } catch (RuntimeException $e) {
            $this->assertStringContainsString('cross-space', $e->getMessage());
            $this->assertStringNotContainsString('CROSSSLOT', $e->getMessage());
        }
    }

    public function test_model_update_invalidates_all_declared_model_spaces(): void
    {
        $author = SpacedAuthor::create(['name' => 'Ann']);
        $post = MultiSpacePost::create(['title' => 'First', 'author_id' => $author->id]);

        $this->assertSame('First', MultiSpacePost::query()->space('content')->get()->first()->title);
        $this->assertSame('First', MultiSpacePost::query()->space('reporting')->get()->first()->title);

        $this->assertAnyKeysForHashTag('nc:content', 'test:query:*');
        $this->assertAnyKeysForHashTag('nc:reporting', 'test:query:*');

        $post->update(['title' => 'Second']);

        $this->assertSame('Second', MultiSpacePost::query()->space('content')->get()->first()->title);
        $this->assertSame('Second', MultiSpacePost::query()->space('reporting')->get()->first()->title);
    }

    public function test_through_relation_cache_uses_related_model_space_on_cluster(): void
    {
        $country = ReportingCountry::create(['name' => 'Australia']);
        $author = SpacedAuthor::create(['name' => 'Alice', 'country_id' => $country->id]);
        $post = SpacedPost::create(['title' => 'Hello', 'author_id' => $author->id]);

        $this->assertSame(['Hello'], $this->assertNoCrossSlot(fn() => $country->spacedPosts()->get()->pluck('title')->all()));
        $this->assertSame(['Hello'], $country->spacedPosts()->get()->pluck('title')->all());

        $this->assertAnyKeysForHashTag('nc:content', 'test:through:*');
        $this->assertNoKeysForHashTag('nc', 'test:through:*');

        $post->update(['title' => 'Updated']);

        $this->assertSame(['Updated'], $country->spacedPosts()->get()->pluck('title')->all());
    }

    public function test_pivot_cache_uses_related_model_space_on_cluster(): void
    {
        $author = SpacedAuthor::create(['name' => 'Ann']);
        $post = SpacedPost::create(['title' => 'First', 'author_id' => $author->id]);
        $firstTag = CatalogTag::create(['name' => 'First']);
        $secondTag = CatalogTag::create(['name' => 'Second']);

        $post->catalogTags()->attach($firstTag->id);

        $first = $this->assertNoCrossSlot(fn() => SpacedPost::query()
            ->with('catalogTags')
            ->get()
            ->first()
            ->catalogTags
            ->pluck('name')
            ->all());

        $this->assertSame(['First'], $first);
        $this->assertAnyKeysForHashTag('nc:catalog', 'test:pivot:*');
        $this->assertNoKeysForHashTag('nc:content', 'test:pivot:*');

        $post->catalogTags()->attach($secondTag->id);

        $second = SpacedPost::query()
            ->with('catalogTags')
            ->get()
            ->first()
            ->catalogTags
            ->pluck('name')
            ->sort()
            ->values()
            ->all();

        $this->assertSame(['First', 'Second'], $second);
    }

    public function test_flush_space_clears_only_that_space_on_cluster(): void
    {
        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'Article', 'author_id' => $author->id]);
        CatalogTag::create(['name' => 'Widgets']);
        ReportingCountry::create(['name' => 'Atlantis']);

        SpacedPost::query()->get();
        CatalogTag::query()->get();
        ReportingCountry::query()->get();

        $this->assertAnyKeysForHashTag('nc:content', 'test:*');
        $this->assertAnyKeysForHashTag('nc:catalog', 'test:*');
        $this->assertAnyKeysForHashTag('nc:reporting', 'test:*');

        $this->assertNoCrossSlot(fn() => $this->cacheManager()->flushAll('content'));

        $this->assertNoKeysForHashTag('nc:content', 'test:*');
        $this->assertAnyKeysForHashTag('nc:catalog', 'test:*');
        $this->assertAnyKeysForHashTag('nc:reporting', 'test:*');
    }

    public function test_flush_all_clears_default_and_known_spaces_on_cluster(): void
    {
        Author::create(['name' => 'Default']);
        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'Article', 'author_id' => $author->id]);
        CatalogTag::create(['name' => 'Widgets']);
        ReportingCountry::create(['name' => 'Atlantis']);

        Author::query()->get();
        SpacedPost::query()->get();
        CatalogTag::query()->get();
        ReportingCountry::query()->get();

        $this->assertAnyKeysForHashTag('nc', 'test:*');
        $this->assertAnyKeysForHashTag('nc:content', 'test:*');
        $this->assertAnyKeysForHashTag('nc:catalog', 'test:*');
        $this->assertAnyKeysForHashTag('nc:reporting', 'test:*');

        $this->assertNoCrossSlot(fn() => $this->cacheManager()->flushAll());

        $this->assertNoKeysForHashTag('nc', 'test:*');
        $this->assertNoKeysForHashTag('nc:content', 'test:*');
        $this->assertNoKeysForHashTag('nc:catalog', 'test:*');
        $this->assertNoKeysForHashTag('nc:reporting', 'test:*');
    }
}
