<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\TableIdentity;

final class PublishRaceTest extends TestCase
{
    private int $authorId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->authorId = (int) Author::query()->create(['name' => 'Author'])->getKey();

        $rows = [];

        for ($index = 1; $index <= 20; $index++) {
            $rows[] = [
                'id' => $index,
                'title' => "Post {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        RawPost::query()->toBase()->insert($rows);
    }

    private function tableIdentity(string $table): TableIdentity
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $table);

        $this->assertNotNull($identity);

        return $identity;
    }

    private function bumpVersionDuring(string $sqlNeedle, callable $callback): mixed
    {
        $bumped = false;
        $versionKey = $this->cacheKeys()->version($this->tableIdentity('posts'));

        DB::listen(function ($query) use (&$bumped, $sqlNeedle, $versionKey): void {
            if ($bumped || !str_contains($query->sql, $sqlNeedle)) {
                return;
            }

            $bumped = true;
            $this->cacheStore()->increment($versionKey);
        });

        $result = $callback();

        $this->assertTrue($bumped, 'expected the miss to reach the database');

        return $result;
    }

    public function test_a_version_bump_during_the_build_leaves_no_canonical_entry(): void
    {
        $rows = $this->bumpVersionDuring(
            'select * from "posts"',
            fn() => RawPost::query()->toBase()->get(),
        );

        $this->assertCount(20, $rows, 'the caller still gets its rows');
        $this->assertSame(
            [],
            $this->cacheQueryKeysWithField('m'),
            'the membership must not be published against a version that already moved',
        );
    }

    public function test_the_read_after_a_raced_build_is_a_miss_not_stale_data(): void
    {
        $this->bumpVersionDuring(
            'select * from "posts"',
            fn() => RawPost::query()->toBase()->get(),
        );

        RawPost::query()->toBase()->where('id', 3)->update(['title' => 'changed']);

        $rows = collect(RawPost::query()->toBase()->get());

        $this->assertSame('changed', $rows->firstWhere('id', 3)->title);
    }

    public function test_a_version_bump_during_a_result_build_leaves_no_entry(): void
    {
        $count = $this->bumpVersionDuring(
            'select count(*)',
            fn() => RawPost::query()->toBase()->count(),
        );

        $this->assertSame(20, $count);
        $this->assertSame(
            [],
            $this->cacheQueryKeysWithField('r'),
            'the result payload must not be published against a moved version',
        );
    }

    public function test_the_lease_is_released_when_the_publish_guard_rejects(): void
    {
        $this->bumpVersionDuring(
            'select * from "posts"',
            fn() => RawPost::query()->toBase()->get(),
        );

        $this->assertSame(
            [],
            $this->cacheKeysMatching(':build:'),
            'a rejected publish must not leak the build lease',
        );
    }
}
