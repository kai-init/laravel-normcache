<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Builder;
use Illuminate\Database\SQLiteConnection;
use Mockery;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\UnitTestCase;

final class TableIdentityResolverTest extends UnitTestCase
{
    public function test_postgres_uses_the_live_effective_schema(): void
    {
        $connection = $this->postgresConnection('tenant');

        $identity = app(TableIdentityResolver::class)->resolve($connection, 'posts');

        $this->assertSame('tenant', $identity?->schema);
    }

    public function test_effective_schema_is_memoized_until_explicitly_cleared(): void
    {
        $builder = Mockery::mock(Builder::class);
        $builder->shouldReceive('getCurrentSchemaName')
            ->twice()
            ->andReturn('tenant', 'other');
        $connection = $this->postgresConnection(null, $builder);
        $resolver = app(TableIdentityResolver::class);

        $this->assertSame('tenant', $resolver->resolve($connection, 'posts')?->schema);
        $this->assertSame('tenant', $resolver->resolve($connection, 'posts')?->schema);
        $resolver->clear('testing');
        $this->assertSame('other', $resolver->resolve($connection, 'posts')?->schema);
    }

    public function test_failed_view_metadata_lookup_is_retried(): void
    {
        $builder = Mockery::mock(Builder::class);
        $builder->shouldReceive('getCurrentSchemaName')->once()->andReturn('public');
        $viewCalls = 0;
        $builder->shouldReceive('getViews')
            ->twice()
            ->andReturnUsing(function () use (&$viewCalls): array {
                if ($viewCalls++ === 0) {
                    throw new \RuntimeException('denied');
                }

                return [];
            });
        $connection = $this->postgresConnection(null, $builder);
        $resolver = app(TableIdentityResolver::class);

        $this->assertNull($resolver->resolve($connection, 'posts'));
        $this->assertSame('public', $resolver->resolve($connection, 'posts')?->schema);
    }

    public function test_view_list_is_shared_by_tables_in_the_same_schema(): void
    {
        $builder = Mockery::mock(Builder::class);
        $builder->shouldReceive('getCurrentSchemaName')->once()->andReturn('public');
        $builder->shouldReceive('getViews')->once()->andReturn([
            ['name' => 'post_summary'],
        ]);
        $connection = $this->postgresConnection(null, $builder);
        $resolver = app(TableIdentityResolver::class);

        $this->assertFalse($resolver->resolve($connection, 'posts')?->isView);
        $this->assertTrue($resolver->resolve($connection, 'post_summary')?->isView);
    }

    public function test_failed_effective_schema_lookup_is_retried(): void
    {
        $builder = Mockery::mock(Builder::class);
        $schemaCalls = 0;
        $builder->shouldReceive('getCurrentSchemaName')
            ->twice()
            ->andReturnUsing(function () use (&$schemaCalls): string {
                if ($schemaCalls++ === 0) {
                    throw new \RuntimeException('denied');
                }

                return 'public';
            });
        $builder->shouldReceive('getViews')->once()->andReturn([]);
        $connection = $this->postgresConnection(null, $builder);
        $resolver = app(TableIdentityResolver::class);

        $this->assertNull($resolver->resolve($connection, 'posts'));
        $this->assertSame('public', $resolver->resolve($connection, 'posts')?->schema);
    }

    public function test_a_recycled_connection_object_id_does_not_resolve_to_the_previous_database(): void
    {
        $resolver = app(TableIdentityResolver::class);
        $first = $this->sqliteConnection('tenant_a');
        $recycledId = spl_object_id($first);

        $this->assertSame(
            realpath($this->databasePath('tenant_a')),
            $resolver->resolve($first, 'posts')?->database,
        );

        $path = $this->databasePath('tenant_b');
        touch($path);
        $pdo = new \PDO('sqlite:' . $path);

        unset($first);
        gc_collect_cycles();

        $this->assertTrue(
            $this->reclaimObjectId($recycledId),
            'PHP never reissued the freed object id, so the collision was not exercised.',
        );

        $second = new SQLiteConnection($pdo, $path, '', ['name' => 'tenant', 'driver' => 'sqlite']);

        $this->assertSame($recycledId, spl_object_id($second));
        $this->assertSame(realpath($path), $resolver->resolve($second, 'posts')?->database);
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
            $this->sqlServerConnection(),
            '[Order Details]',
        );

        $this->assertNull($identity);
    }

    public function test_four_part_sql_server_sources_bypass_table_identity_resolution(): void
    {
        $identity = app(TableIdentityResolver::class)->resolve(
            $this->sqlServerConnection(),
            'server.database.schema.posts',
        );

        $this->assertNull($identity);
    }

    public function test_metadata_is_released_when_its_connection_is_discarded(): void
    {
        $resolver = app(TableIdentityResolver::class);
        $connection = $this->sqliteConnection('tenant_a');

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

    /** @var list<object> */
    private array $reclaimFiller = [];

    /**
     * Leaves $id at the head of PHP's object free list, so the caller's next
     * allocation lands on it. Fillers are held on the test instance: releasing
     * them would push their ids ahead of $id and lose the slot.
     */
    private function reclaimObjectId(int $id): bool
    {
        for ($attempt = 0; $attempt < 20_000; $attempt++) {
            $probe = new \stdClass;

            if (spl_object_id($probe) === $id) {
                unset($probe);

                return true;
            }

            $this->reclaimFiller[] = $probe;
        }

        return false;
    }

    private function databasePath(string $tenant): string
    {
        return sys_get_temp_dir() . '/normcache-' . $tenant . '.sqlite';
    }

    private function sqliteConnection(string $tenant): Connection
    {
        $path = $this->databasePath($tenant);
        touch($path);

        return new SQLiteConnection(
            new \PDO('sqlite:' . $path),
            $path,
            '',
            ['name' => 'tenant', 'driver' => 'sqlite'],
        );
    }

    private function postgresConnection(
        ?string $schema,
        ?Builder $builder = null,
    ): Connection {
        $builder ??= Mockery::mock(Builder::class);
        $builder->shouldReceive('getViews')->byDefault()->andReturn([]);

        if ($schema !== null) {
            $builder->shouldReceive('getCurrentSchemaName')
                ->once()
                ->andReturn($schema);
        }

        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getDriverName')->andReturn('pgsql');
        $connection->shouldReceive('getName')->andReturn('testing');
        $connection->shouldReceive('getDatabaseName')->andReturn('app');
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')
            ->andReturn(['name' => 'testing']);
        $connection->shouldReceive('getSchemaBuilder')->andReturn($builder);

        return $connection;
    }

    private function sqlServerConnection(): Connection
    {
        $builder = Mockery::mock(Builder::class);
        $builder->shouldReceive('getCurrentSchemaName')->andReturn('dbo');
        $builder->shouldReceive('getViews')->andReturn([]);

        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getDriverName')->andReturn('sqlsrv');
        $connection->shouldReceive('getName')->andReturn('testing');
        $connection->shouldReceive('getDatabaseName')->andReturn('app');
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')->andReturn(['name' => 'testing']);
        $connection->shouldReceive('getSchemaBuilder')->andReturn($builder);

        return $connection;
    }
}
