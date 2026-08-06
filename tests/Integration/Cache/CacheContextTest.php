<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;

final class CacheContextTest extends TestCase
{
    public function test_contexts_isolate_identical_direct_queries(): void
    {
        $author = Author::create(['name' => 'Author']);
        $post = Post::create(['title' => 'Tenant A', 'author_id' => $author->id]);
        $read = static fn(string $context): ?Post => Post::query()
            ->cacheContext($context)
            ->whereKey($post->id)
            ->first();

        $this->assertSame('Tenant A', $read('tenant:a')?->title);
        DB::connection()->update(
            'update posts set title = ? where id = ?',
            ['Tenant B', $post->id],
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $tenantB = $read('tenant:b');
        $tenantA = $read('tenant:a');
        DB::disableQueryLog();

        $this->assertSame('Tenant B', $tenantB?->title);
        $this->assertSame('Tenant A', $tenantA?->title);
        $this->assertCount(1, DB::getQueryLog());
        $this->assertSame([], $this->cacheKeysMatching(':r:g'));
        $this->assertCount(2, $this->cacheQueryKeysWithField('r'));
    }

    public function test_contexts_isolate_identical_canonical_shapes(): void
    {
        $author = Author::create(['name' => 'Tenant A']);
        $read = static fn(string $context): string => (string) Author::query()
            ->cacheContext($context)
            ->orderBy('id')
            ->value('name');

        $this->assertSame('Tenant A', $read('tenant:a'));
        DB::connection()->update(
            'update authors set name = ? where id = ?',
            ['Tenant B', $author->id],
        );

        $this->assertSame('Tenant B', $read('tenant:b'));
        $this->assertSame('Tenant A', $read('tenant:a'));
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
        $this->assertSame([], $this->cacheKeysMatching(':r:g'));
    }

    public function test_tag_flushes_still_invalidate_context_namespaces(): void
    {
        $author = Author::create(['name' => 'Before']);
        $read = static fn(): string => (string) Author::query()
            ->cacheContext('tenant:a')
            ->tag('homepage')
            ->value('name');

        $this->assertSame('Before', $read());
        DB::connection()->update(
            'update authors set name = ? where id = ?',
            ['After', $author->id],
        );
        $this->assertSame('Before', $read());

        $this->cacheManager()->flushTag('homepage');

        $this->assertSame('After', $read());
    }

    public function test_cache_context_rejects_invalid_values(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('NormCache cache context');

        DB::table('posts')->cacheContext('');
    }
}
