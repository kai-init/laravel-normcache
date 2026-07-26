<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;

final class CanonicalReadTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Canonical',
            'views' => 10,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_canonical_list_populates_rows_reused_by_direct_pk_reads(): void
    {
        DB::table('posts')->orderBy('id')->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertSame([], DB::getQueryLog());

        $this->assertNotEmpty($this->cacheKeysMatching(':m:v'));
        $this->assertNotEmpty($this->cacheKeysMatching(':r:g'));
    }

    public function test_narrow_projection_uses_an_exact_result_without_widening_sql(): void
    {
        $cold = DB::table('posts')
            ->where('id', $this->postId)
            ->select('title as heading')
            ->first();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = DB::table('posts')
            ->where('id', $this->postId)
            ->select('title as heading')
            ->first();
        DB::disableQueryLog();

        $this->assertSame(['heading' => 'Canonical'], (array) $cold);
        $this->assertSame((array) $cold, (array) $warm);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_missing_canonical_rows_are_repaired_by_primary_key_batch(): void
    {
        $expected = DB::table('posts')->orderBy('id')->get();
        $rowKey = $this->cacheKeysMatching(':r:g')[0] ?? null;

        $this->assertIsString($rowKey);
        $this->cacheStore()->delete($rowKey);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = DB::table('posts')->orderBy('id')->get();
        DB::disableQueryLog();

        $this->assertSame($expected->map(fn($row) => (array) $row)->all(), $actual->map(fn($row) => (array) $row)->all());
        $this->assertCount(1, DB::getQueryLog());
        $this->assertStringContainsString('where "id" in', strtolower(DB::getQueryLog()[0]['query']));
    }

    public function test_direct_primary_key_rows_apply_builtin_soft_delete_visibility(): void
    {
        Post::query()->whereKey($this->postId)->delete();
        $trashed = Post::withTrashed()->findOrFail($this->postId);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $default = Post::query()->find($this->postId);
        $only = Post::onlyTrashed()->find($this->postId);
        DB::disableQueryLog();

        $this->assertNull($default);
        $this->assertSame($trashed->getKey(), $only?->getKey());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_or_deleted_at_predicate_is_not_misclassified_as_a_direct_lookup(): void
    {
        $second = Post::query()->create([
            'title' => 'Second',
            'views' => 0,
            'published' => true,
            'author_id' => DB::table('authors')->value('id'),
        ]);

        $read = fn() => Post::withTrashed()
            ->whereKey($this->postId)
            ->orWhereNull('deleted_at')
            ->get();

        $this->assertCount(2, $read());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $ids = $read()->modelKeys();
        DB::disableQueryLog();

        $this->assertContains($this->postId, $ids);
        $this->assertContains($second->getKey(), $ids);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_string_primary_keys_share_canonical_rows_with_direct_reads(): void
    {
        $id = 'uuid:with/{delimiters}';
        UuidItem::query()->create(['id' => $id, 'name' => 'String key']);
        UuidItem::query()->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $item = UuidItem::query()->findOrFail($id);
        DB::disableQueryLog();

        $this->assertSame('String key', $item->name);
        $this->assertSame([], DB::getQueryLog());
    }
}
