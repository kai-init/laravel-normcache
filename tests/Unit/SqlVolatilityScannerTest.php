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
            'MySQL advisory lock' => ['GET_LOCK(\'normcache\', 0)'],
            'MySQL advisory unlock' => ['RELEASE_LOCK(\'normcache\')'],
            'PostgreSQL advisory lock' => ['pg_try_advisory_xact_lock(42)'],
            'PostgreSQL advisory unlock' => ['pg_advisory_unlock_all()'],
            'SQL Server advisory lock' => ['sp_getapplock(\'normcache\', \'Exclusive\')'],
            'schema-qualified volatile function' => ['pg_catalog.random()'],
            'volatile keyword' => ['CURRENT_TIMESTAMP'],
            'sequence syntax' => ['NEXT VALUE FOR dbo.order_seq'],
            'multiline sequence syntax' => ["NEXT\nVALUE FOR dbo.order_seq"],
            'sequence function in projection' => ['select nextval(\'order_seq\') as id'],
            'sequence function in aggregate' => ['select max(nextval(\'order_seq\'))'],
            'sequence function in ordering' => ['select id from posts order by nextval(\'order_seq\')'],
            'sequence function in predicate' => ['select id from posts where id < nextval(\'order_seq\')'],
            'MySQL session variable' => ['@tenant_id = 42'],
            'MySQL connection state' => ['CONNECTION_ID()'],
            'PostgreSQL connection state' => ['current_setting(\'application_name\')'],
            'SQL Server connection state' => ['SESSION_CONTEXT(N\'tenant\')'],
            'argument-dependent function' => ['datetime(\'now\', \'+1 day\')'],
            'unknown function' => ['vendor_schema.custom_score(users.id)'],
            'quoted unknown function' => ['"custom_score"(users.id)'],
            'unknown nested function' => ['coalesce(custom_score(id), 0)'],
            'commented unknown function' => ['custom_score/**/(id)'],
            'SQL comment' => ['select count(*) /* stable but opaque */ from posts'],
            'quoted SQL keyword function' => ['"select"(id)'],
        ];
    }

    #[DataProvider('deterministicExpressions')]
    public function test_it_accepts_ordinary_sql_and_known_deterministic_functions(
        string $sql,
    ): void {
        $this->assertFalse((new SqlVolatilityScanner)->isVolatile($sql));
    }

    public static function deterministicExpressions(): array
    {
        return [
            'deterministic functions' => ['ROUND(AVG(order_items.price), 2)'],
            'fixed date argument' => ['date(\'2026-08-04\')'],
            'ordinary query' => ['select * from "posts" where "published" = ? order by "created_at" asc'],
        ];
    }
}
