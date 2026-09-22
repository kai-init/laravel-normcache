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
            ['nc-table', 'tenant', 'pgsql', 'app', 'public', 'acme_posts'],
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

    public function test_database_sources_are_part_of_physical_identity(): void
    {
        $one = TableIdentity::fromParts('mysql', 'shard-a', 'app', 'app', '', 'posts');
        $two = TableIdentity::fromParts('mysql', 'shard-b', 'app', 'app', '', 'posts');
        $alias = TableIdentity::fromParts(
            'mysql',
            'shard-b',
            'app',
            'app',
            '',
            'posts',
            sourceScope: 'shard-a',
        );

        $this->assertNotSame($one->hash, $two->hash);
        $this->assertSame($one->hash, $alias->hash);
    }

    public function test_physical_table_identity_is_independent_of_prefix_spelling(): void
    {
        $prefixed = TableIdentity::fromParts('mysql', 'one', 'app', 'app', 'pre_', 'posts', 'shared');
        $plain = TableIdentity::fromParts('mysql', 'two', 'app', 'app', '', 'pre_posts', 'shared');
        $other = TableIdentity::fromParts('mysql', 'two', 'app', 'app', '', 'posts', 'shared');

        $this->assertSame($prefixed->hash, $plain->hash);
        $this->assertNotSame($prefixed->hash, $other->hash);
        $this->assertSame('app.posts', $prefixed->qualifiedTable());
        $this->assertSame('app.pre_posts', $plain->qualifiedTable());
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
