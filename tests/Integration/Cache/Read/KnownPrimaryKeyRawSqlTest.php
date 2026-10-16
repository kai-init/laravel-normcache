<?php

namespace NormCache\Tests\Integration\Cache\Read;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Database\QueryBuilder;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;

final class KnownPrimaryKeyRawSqlTest extends TestCase
{
    private const TABLE = 'normcache_known_primary_key_raw_sql';

    protected function tearDown(): void
    {
        if (in_array(DB::connection()->getDriverName(), ['mysql', 'mariadb'], true)) {
            Schema::dropIfExists(self::TABLE);
        }

        parent::tearDown();
    }

    public function test_mysql_repairs_native_negative_and_big_integer_candidates_with_exact_literals(): void
    {
        $this->requireMysql();
        $this->createSignedTable();
        $ids = [-7, 9007199254740993];
        DB::table(self::TABLE)->insert([
            ['id' => $ids[0], 'label' => 'negative'],
            ['id' => $ids[1], 'label' => 'large'],
        ]);

        [$rows, $queries] = $this->observe(fn() => $this->rawSqlQuery()->whereIn('id', [$ids[1], $ids[0]])
            ->orderBy('id')->get());

        $this->assertSame(['-7', '9007199254740993'], $rows->pluck('id')->map('strval')->all());
        $this->assertCount(1, $queries);
        $payloadSelect = $queries[0];
        $this->assertStringStartsWith('select * from ', strtolower($payloadSelect['query']));
        $this->assertMatchesRegularExpression('/\\bin\\s*\\(-7,\\s*9007199254740993\\)/i', $payloadSelect['query']);
        $this->assertSame([], $payloadSelect['bindings']);
    }

    public function test_mysql_overflow_candidate_uses_lossless_bound_repair(): void
    {
        $this->requireMysql();
        Schema::create(self::TABLE, static function (Blueprint $table): void {
            $table->unsignedBigInteger('id')->primary();
            $table->string('label');
        });
        $id = '9223372036854775808';
        DB::table(self::TABLE)->insert(['id' => $id, 'label' => 'overflow']);

        [$rows, $queries] = $this->observe(fn() => $this->rawSqlQuery()->whereIn('id', [$id])->orderBy('id')->get());

        $this->assertSame([$id], $rows->pluck('id')->map('strval')->all());
        $this->assertCount(2, $queries);
        $this->assertSame([$id], $queries[0]['bindings']);
        $this->assertStringStartsWith('select * from ', strtolower($queries[1]['query']));
        $this->assertMatchesRegularExpression('/\\bin\\s*\\(\\?\\)/i', $queries[1]['query']);
        $this->assertSame([$id], $queries[1]['bindings']);
    }

    private function requireMysql(): void
    {
        if (!in_array(DB::connection()->getDriverName(), ['mysql', 'mariadb'], true)) {
            $this->markTestSkipped('These SQL literal and unsigned BIGINT checks require MySQL or MariaDB.');
        }

        Schema::dropIfExists(self::TABLE);
    }

    private function createSignedTable(): void
    {
        Schema::create(self::TABLE, static function (Blueprint $table): void {
            $table->bigInteger('id')->primary();
            $table->string('label');
        });
    }

    private function rawSqlQuery(): QueryBuilder
    {
        return RawPost::query()->toBase()->from(self::TABLE);
    }

    /** @return array{mixed, list<array{query: string, bindings: array}>} */
    private function observe(callable $callback): array
    {
        DB::flushQueryLog();
        DB::enableQueryLog();
        try {
            $result = $callback();
            $queries = DB::getQueryLog();
        } finally {
            DB::disableQueryLog();
        }

        return [$result, $queries];
    }
}
