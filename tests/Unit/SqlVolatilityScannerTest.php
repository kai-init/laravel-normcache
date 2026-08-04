<?php

namespace NormCache\Tests\Unit;

use NormCache\Planning\SqlVolatilityScanner;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class SqlVolatilityScannerTest extends TestCase
{
    #[DataProvider('volatileExpressions')]
    public function test_it_detects_known_volatile_expressions(string $sql): void
    {
        $this->assertTrue((new SqlVolatilityScanner)->isVolatile($sql));
    }

    public static function volatileExpressions(): array
    {
        return [
            'volatile function' => ['GET_LOCK(\'normcache\', 0)'],
            'schema-qualified volatile function' => ['pg_catalog.random()'],
            'volatile keyword' => ['CURRENT_TIMESTAMP'],
            'sequence syntax' => ['NEXT VALUE FOR dbo.order_seq'],
            'multiline sequence syntax' => ["NEXT\nVALUE FOR dbo.order_seq"],
            'session variable' => ['@tenant_id = 42'],
            'argument-dependent function' => ['datetime(\'now\', \'+1 day\')'],
        ];
    }

    #[DataProvider('deterministicExpressions')]
    public function test_it_ignores_ordinary_sql_and_unknown_functions(
        string $sql,
    ): void {
        $this->assertFalse((new SqlVolatilityScanner)->isVolatile($sql));
    }

    public static function deterministicExpressions(): array
    {
        return [
            'deterministic functions' => ['ROUND(AVG(order_items.price), 2)'],
            'unknown function' => ['vendor_schema.custom_score(users.id)'],
            'quoted unknown function' => ['"custom_score"(users.id)'],
            'unknown nested function' => ['coalesce(custom_score(id), 0)'],
            'fixed date argument' => ['date(\'2026-08-04\')'],
            'ordinary query' => ['select * from "posts" where "published" = ? order by "created_at" asc'],
        ];
    }
}
