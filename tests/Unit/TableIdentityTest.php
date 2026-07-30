<?php

namespace NormCache\Tests\Unit;

use NormCache\Tests\UnitTestCase;
use NormCache\Values\TableIdentity;

final class TableIdentityTest extends UnitTestCase
{
    public function test_hashes_exact_length_prefixed_identity(): void
    {
        $identity = TableIdentity::fromParts(
            driver: 'pgsql',
            connection: 'tenant',
            database: 'app',
            schema: 'public',
            prefix: 'acme_',
            table: 'posts',
        );

        $encoded = implode('', array_map(
            static fn(string $value): string => strlen($value) . ':' . $value,
            ['nc-table', 'pgsql', 'tenant', 'app', 'public', 'acme_', 'posts'],
        ));

        $this->assertSame($encoded, $identity->encoded);
        $this->assertSame(hash('xxh128', $encoded), $identity->hash);
        $this->assertSame(32, strlen($identity->hash));
    }

    public function test_aliases_are_not_part_of_physical_identity(): void
    {
        $one = TableIdentity::fromParts('mysql', 'main', 'app', 'app', '', 'posts');
        $two = TableIdentity::fromParts('mysql', 'main', 'app', 'app', '', 'posts');

        $this->assertSame($one->hash, $two->hash);
    }

    public function test_sqlite_repair_source_keeps_attached_schema(): void
    {
        $identity = TableIdentity::fromParts(
            'sqlite',
            'testing',
            '/tmp/testing.sqlite',
            'tenant',
            '',
            'posts',
        );

        $this->assertSame('tenant.posts', $identity->qualifiedTable());
    }

    public function test_sql_server_repair_source_keeps_database_and_schema(): void
    {
        $identity = TableIdentity::fromParts(
            'sqlsrv',
            'tenant',
            'catalog',
            'dbo',
            '',
            'posts',
        );

        $this->assertSame('catalog.dbo.posts', $identity->qualifiedTable());
    }
}
