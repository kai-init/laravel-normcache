<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;

final class EntryKeyStabilityTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) RawPost::query()->toBase()->insertGetId([
            'title' => 'Post 0',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        for ($i = 1; $i < 3; $i++) {
            RawPost::query()->toBase()->insert([
                'title' => "Post {$i}",
                'views' => $i,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    public function test_a_version_bump_neither_orphans_nor_resurrects_an_entry(): void
    {
        $query = fn() => RawPost::query()->toBase()->orderBy('id')->get();

        $query();
        $before = $this->cacheQueryKeysWithField('m');

        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Updated']);

        $this->assertSame('Updated', collect($query())->firstWhere('id', $this->postId)->title);

        $this->assertSame(
            $before,
            $this->cacheQueryKeysWithField('m'),
            'a version bump must reuse the entry key rather than orphaning it under a new one',
        );
    }
}
