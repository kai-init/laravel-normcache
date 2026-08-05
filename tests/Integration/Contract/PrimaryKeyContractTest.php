<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;

/**
 * Contract tests: primary-key lookups (find, whereKey, where id) must return identical
 * results on the native path (withoutCache), cold-cache path, and warm-cache path,
 * particularly regarding result ordering and fast-path bypass.
 */
final class PrimaryKeyContractTest extends TestCase
{
    public function test_where_in_primary_key_order(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::create(['name' => 'Carol']);

        $this->contract(
            fn() => Author::whereIn('id', [3, 1, 2])->get(),
            fn() => Author::withoutCache()->whereIn('id', [3, 1, 2])->get(),
        );

        $this->contract(
            fn() => Author::whereIn('id', [2, 3, 1])->get(),
            fn() => Author::withoutCache()->whereIn('id', [2, 3, 1])->get(),
        );
    }

    public function test_where_in_primary_key_with_explicit_order(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::create(['name' => 'Carol']);

        $this->contract(
            fn() => Author::whereIn('id', [3, 1, 2])->orderByDesc('id')->get(),
            fn() => Author::withoutCache()->whereIn('id', [3, 1, 2])->orderByDesc('id')->get(),
        );
    }

    public function test_where_in_uuid_primary_key_order(): void
    {
        UuidItem::create(['id' => 'b8f8702c-4734-45e0-a548-18e3c66f6f9c', 'name' => 'B']);
        UuidItem::create(['id' => 'a1f8702c-4734-45e0-a548-18e3c66f6f9c', 'name' => 'A']);
        UuidItem::create(['id' => 'c1f8702c-4734-45e0-a548-18e3c66f6f9c', 'name' => 'C']);

        $ids = [
            'c1f8702c-4734-45e0-a548-18e3c66f6f9c',
            'a1f8702c-4734-45e0-a548-18e3c66f6f9c',
            'b8f8702c-4734-45e0-a548-18e3c66f6f9c',
        ];

        $this->contract(
            fn() => UuidItem::whereIn('id', $ids)->get(),
            fn() => UuidItem::withoutCache()->whereIn('id', $ids)->get(),
        );
    }

    public function test_unsigned_bigint_values_above_php_int_max_use_canonical_rows(): void
    {
        if (!in_array(DB::connection()->getDriverName(), ['mysql', 'mariadb'], true)) {
            $this->markTestSkipped('Requires MySQL or MariaDB unsigned BIGINT support.');
        }

        Schema::create('unsigned_records', function (Blueprint $table): void {
            $table->unsignedBigInteger('id')->primary();
            $table->string('name');
        });

        try {
            $id = '18446744073709551615';
            DB::table('unsigned_records')->insert(['id' => $id, 'name' => 'Original']);
            $direct = static fn() => DB::table('unsigned_records')->where('id', $id)->first();
            $canonical = static fn() => DB::table('unsigned_records')->orderBy('id')->get();

            $this->assertSame('Original', $direct()?->name);

            DB::flushQueryLog();
            DB::enableQueryLog();
            $this->assertSame('Original', $direct()?->name);
            DB::disableQueryLog();
            $this->assertSame([], DB::getQueryLog());

            $this->assertSame($id, (string) $canonical()->first()?->id);
            $this->deleteResultOverlays();
            $identity = app(TableIdentityResolver::class)
                ->resolve(DB::connection(), 'unsigned_records');
            $this->assertNotNull($identity);
            $generation = $this->cacheStore()->getRaw(
                $this->cacheKeys()->generation($identity),
            ) ?? '0';
            $rowKey = $this->cacheKeys()->row($identity, $generation, 'i:' . $id);
            $this->assertNotNull($this->cacheStore()->getRaw($rowKey));
            $this->cacheStore()->delete($rowKey);

            DB::flushQueryLog();
            DB::enableQueryLog();
            $repaired = $canonical();
            DB::disableQueryLog();

            $this->assertSame($id, (string) $repaired->first()?->id);
            $this->assertCount(1, DB::getQueryLog());

            DB::table('unsigned_records')->where('id', $id)->update(['name' => 'Updated']);

            $this->assertSame('Updated', $direct()?->name);
        } finally {
            Schema::dropIfExists('unsigned_records');
            app(TableIdentityResolver::class)->clear();
        }
    }
}
