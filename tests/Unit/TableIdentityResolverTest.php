<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\SQLiteConnection;
use Mockery;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\UnitTestCase;

final class TableIdentityResolverTest extends UnitTestCase
{
    public function test_postgres_uses_configured_schema_without_schema_introspection(): void
    {
        $connection = $this->connection('pgsql', ['schema' => 'tenant']);
        $connection->shouldNotReceive('getSchemaBuilder');

        $identity = app(TableIdentityResolver::class)->resolve($connection, 'posts');

        $this->assertSame('tenant', $identity?->schema);
    }

    public function test_qualified_postgres_table_supplies_its_schema(): void
    {
        $identity = app(TableIdentityResolver::class)->resolve(
            $this->connection('pgsql', ['schema' => 'public']),
            'tenant.posts as p',
        );

        $this->assertSame('tenant', $identity?->schema);
        $this->assertSame('posts', $identity?->table);
    }

    public function test_unqualified_sources_are_resolved_without_checking_table_existence(): void
    {
        $connection = $this->connection('pgsql', ['schema' => 'public']);
        $connection->shouldNotReceive('getSchemaBuilder');

        $identity = app(TableIdentityResolver::class)->resolve($connection, 'not_migrated_yet');

        $this->assertSame('not_migrated_yet', $identity?->table);
    }

    public function test_sqlite_case_and_main_schema_variants_share_one_identity(): void
    {
        $resolver = app(TableIdentityResolver::class);
        $connection = $this->sqliteConnection('case-identity');

        $lower = $resolver->resolve($connection, 'posts');
        $upper = $resolver->resolve($connection, 'POSTS');
        $qualified = $resolver->resolve($connection, 'MAIN.Posts');

        $this->assertNotNull($lower);
        $this->assertSame('main', $lower->schema);
        $this->assertSame('posts', $lower->table);
        $this->assertSame($lower->hash, $upper?->hash);
        $this->assertSame($lower->hash, $qualified?->hash);
    }

    public function test_source_scope_changes_retire_memoized_identities(): void
    {
        $resolver = app(TableIdentityResolver::class);
        $connection = $this->sqliteConnection('source-scope');
        $config = (array) (new \ReflectionProperty(Connection::class, 'config'))->getValue($connection);
        $property = new \ReflectionProperty(Connection::class, 'config');

        $property->setValue($connection, [...$config, 'normcache_scope' => 'tenant-a']);
        $first = $resolver->resolve($connection, 'posts');

        $property->setValue($connection, [...$config, 'normcache_scope' => 'tenant-b']);
        $second = $resolver->resolve($connection, 'posts');

        $this->assertSame('tenant-a', $first?->sourceScope);
        $this->assertSame('tenant-b', $second?->sourceScope);
        $this->assertNotSame($first?->hash, $second?->hash);
    }

    public function test_quoted_identifiers_with_spaces_bypass_table_identity_resolution(): void
    {
        $identity = app(TableIdentityResolver::class)->resolve(
            $this->connection('sqlsrv', ['schema' => 'dbo']),
            '[Order Details]',
        );

        $this->assertNull($identity);
    }

    public function test_four_part_sql_server_sources_bypass_table_identity_resolution(): void
    {
        $identity = app(TableIdentityResolver::class)->resolve(
            $this->connection('sqlsrv', ['schema' => 'dbo']),
            'server.database.schema.posts',
        );

        $this->assertNull($identity);
    }

    public function test_metadata_is_released_when_its_connection_is_discarded(): void
    {
        $resolver = app(TableIdentityResolver::class);
        $connection = $this->sqliteConnection('weak-map');

        $resolver->resolve($connection, 'posts');
        $this->assertSame(1, $this->cachedConnectionCount($resolver));

        unset($connection);
        gc_collect_cycles();

        $this->assertSame(0, $this->cachedConnectionCount($resolver));
    }

    private function cachedConnectionCount(TableIdentityResolver $resolver): int
    {
        $connections = (new \ReflectionProperty($resolver, 'connections'))->getValue($resolver);

        return count($connections);
    }

    private function sqliteConnection(string $tenant): Connection
    {
        $path = sys_get_temp_dir() . '/normcache-' . $tenant . '.sqlite';
        touch($path);

        return new SQLiteConnection(
            new \PDO('sqlite:' . $path),
            $path,
            '',
            ['name' => 'tenant', 'driver' => 'sqlite'],
        );
    }

    /** @param array<string, mixed> $config */
    private function connection(string $driver, array $config): Connection
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getDriverName')->andReturn($driver);
        $connection->shouldReceive('getName')->andReturn('testing');
        $connection->shouldReceive('getDatabaseName')->andReturn('app');
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')->andReturn([
            'name' => 'testing',
            ...$config,
        ]);

        return $connection;
    }
}
