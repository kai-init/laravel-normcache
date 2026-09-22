<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;

final class IdsFirstReadTest extends TestCase
{
    public function test_cold_membership_fetches_ids_then_entities(): void
    {
        $first = Author::create(['name' => 'First']);
        $second = Author::create(['name' => 'Second']);
        $query = fn() => Author::orderByDesc('id')->get();

        [$rows, $queries] = $this->observe($query);

        $this->assertSame([$second->id, $first->id], $rows->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertSame('select "id" from "authors" order by "id" desc', $queries[0]['query']);
        $this->assertStringContainsString('where "id" in (', $queries[1]['query']);
        $this->assertEqualsCanonicalizing([$first->id, $second->id], $queries[1]['bindings']);
        $this->assertWarmCacheHit($query);
    }

    public function test_new_membership_reuses_entities_from_another_query(): void
    {
        $first = Author::create(['name' => 'First']);
        $second = Author::create(['name' => 'Second']);
        Author::orderBy('id')->get();
        $query = fn() => Author::where('id', '>=', $first->id)->orderByDesc('id')->get();

        [$rows, $queries] = $this->observe($query);

        $this->assertSame([$second->id, $first->id], $rows->modelKeys());
        $this->assertCount(1, $queries);
        $this->assertStringStartsWith('select "id"', $queries[0]['query']);
        $this->assertWarmCacheHit($query);
    }

    public function test_only_uncached_entities_are_fetched_after_ids(): void
    {
        $first = Author::create(['name' => 'First']);
        $second = Author::create(['name' => 'Second']);
        Author::find($first->id);

        [$rows, $queries] = $this->observe(fn() => Author::orderByDesc('id')->get());

        $this->assertSame([$second->id, $first->id], $rows->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertSame([$second->id], $queries[1]['bindings']);
    }

    public function test_id_query_retains_alias_filters_order_and_pagination(): void
    {
        Author::create(['name' => 'Skip']);
        $first = Author::create(['name' => 'Match A']);
        $second = Author::create(['name' => 'Match B']);
        Author::create(['name' => 'Match C']);
        $query = fn() => Author::from('authors as a')->select('a.*')
            ->where('a.name', 'like', 'Match%')->orderByDesc('a.name')->offset(1)->limit(2)->get();

        [$rows, $queries] = $this->observe($query);

        $this->assertSame([$second->id, $first->id], $rows->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertStringContainsString('limit 2 offset 1', $queries[0]['query']);
        $this->assertSame(['Match%'], $queries[0]['bindings']);
        $this->assertEqualsCanonicalizing([$first->id, $second->id], $queries[1]['bindings']);
        $this->assertStringNotContainsString('offset', $queries[1]['query']);
        $this->assertWarmCacheHit($query);
    }

    public function test_empty_membership_needs_only_the_id_query(): void
    {
        $query = fn() => Author::orderBy('id')->get();

        [$rows, $queries] = $this->observe($query);

        $this->assertTrue($rows->isEmpty());
        $this->assertCount(1, $queries);
        $this->assertStringStartsWith('select "id"', $queries[0]['query']);
        $this->assertWarmCacheHit($query);
    }

    public function test_both_id_and_entity_queries_use_the_write_connection(): void
    {
        $author = Author::create(['name' => 'Primary']);
        $connection = DB::connection();
        $readPdo = $connection->getRawReadPdo();
        $connection->setReadPdo(static function (): never {
            throw new \RuntimeException('The replica must not serve cache fills.');
        });

        try {
            [$rows, $queries] = $this->observe(fn() => Author::orderBy('id')->get());
        } finally {
            $connection->setReadPdo($readPdo);
        }

        $this->assertSame([$author->id], $rows->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertSame(Author::orderBy('id')->withoutCache()->get()->toArray(), $rows->toArray());
    }

    public function test_string_keys_reuse_existing_entities(): void
    {
        UuidItem::create(['id' => 'a:{}', 'name' => 'A']);
        UuidItem::create(['id' => '42', 'name' => 'B']);
        UuidItem::find('42');

        [$rows, $queries] = $this->observe(fn() => UuidItem::orderByDesc('name')->get());

        $this->assertSame(['42', 'a:{}'], $rows->modelKeys());
        $this->assertCount(2, $queries);
        $this->assertSame(['a:{}'], $queries[1]['bindings']);
    }

    public function test_raw_positional_ordering_keeps_the_original_select_list(): void
    {
        Author::create(['name' => 'Zulu']);
        $alpha = Author::create(['name' => 'Alpha']);
        $query = fn() => Author::orderByRaw('2')->limit(1)->get();

        [$rows, $queries] = $this->observe($query);

        $this->assertSame([$alpha->id], $rows->modelKeys());
        $this->assertCount(1, $queries);
        $this->assertStringStartsWith('select *', $queries[0]['query']);
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
        $this->assertWarmCacheHit($query);
    }

    public function test_write_between_ids_and_entities_discards_the_old_membership(): void
    {
        $author = Author::create(['name' => 'Before']);
        $changed = false;
        DB::listen(function ($event) use ($author, &$changed): void {
            if (!$changed && str_starts_with($event->sql, 'select "id"')) {
                $changed = true;
                $author->update(['name' => 'After']);
            }
        });
        $query = fn() => Author::where('name', 'Before')->get();

        $this->assertTrue($query()->isEmpty());
        $this->assertTrue($changed);
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
        $this->assertSame([], $this->cacheKeysMatching(':build:'));
        $this->assertTrue($query()->isEmpty());
        $this->assertWarmCacheHit($query);
    }

    public function test_write_after_ids_rejects_even_a_fully_cached_entity_set(): void
    {
        $author = Author::create(['name' => 'Before']);
        Author::find($author->id);
        $changed = false;
        DB::listen(function ($event) use (&$changed): void {
            if (!$changed && str_starts_with($event->sql, 'select "id"')) {
                $changed = true;
                Author::create(['name' => 'After']);
            }
        });

        [$rows, $queries] = $this->observe(fn() => Author::orderBy('id')->get());

        $this->assertSame(['Before', 'After'], $rows->pluck('name')->all());
        $this->assertTrue($changed);
        $this->assertStringStartsWith('select *', $queries[array_key_last($queries)]['query']);
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
    }

    public function test_vanished_entity_falls_back_without_publishing_incomplete_membership(): void
    {
        $author = Author::create(['name' => 'Before']);
        $deleted = false;
        DB::listen(function ($event) use ($author, &$deleted): void {
            if (!$deleted && str_starts_with($event->sql, 'select "id"')) {
                $deleted = true;
                DB::table('authors')->where('id', $author->id)->delete();
            }
        });

        $this->assertTrue(Author::orderBy('id')->get()->isEmpty());
        $this->assertTrue($deleted);
        $this->assertSame([], $this->cacheQueryKeysWithField('m'));
        $this->assertSame([], $this->cacheKeysMatching(':build:'));
    }

    private function observe(callable $query): array
    {
        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            return [$query(), DB::getQueryLog()];
        } finally {
            DB::disableQueryLog();
        }
    }
}
