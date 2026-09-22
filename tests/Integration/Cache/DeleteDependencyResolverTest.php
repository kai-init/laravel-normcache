<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\DB;
use Mockery;
use NormCache\Planning\ConnectionSourceResolver;
use NormCache\Planning\DeleteDependencyResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\SchemaCache;
use NormCache\Tests\TestCase;
use NormCache\Values\TableIdentity;

final class DeleteDependencyResolverTest extends TestCase
{
    public function test_graphs_from_the_previous_table_identity_format_are_not_reused(): void
    {
        $connection = DB::connection();
        $oldSignature = TableIdentity::encodeFields([
            ConnectionSourceResolver::resolve($connection),
            $connection->getDriverName(),
            $connection->getDatabaseName(),
            $connection->getTablePrefix(),
            serialize($connection->getConfig('search_path') ?? $connection->getConfig('schema')),
            '0',
        ]);
        $this->cacheStore()->setRawForever(
            $this->cacheKeys()->schema(hash('xxh128', $oldSignature), 'deletes'),
            '[]',
        );
        $parent = app(TableIdentityResolver::class)->resolve($connection, 'authors');

        $this->assertContains(
            'posts',
            $this->affectedTableNames(app(DeleteDependencyResolver::class), $connection, $parent),
        );
    }

    public function test_a_fresh_resolver_reuses_the_graph_without_schema_queries(): void
    {
        $connection = DB::connection();
        $tables = app(TableIdentityResolver::class);
        $parent = $tables->resolve($connection, 'authors');
        $first = app(DeleteDependencyResolver::class)->affectedByDelete($connection, $parent, '0');
        $this->assertNotEmpty($first);
        $fresh = new DeleteDependencyResolver($tables, new SchemaCache($this->cacheStore(), $this->cacheKeys()));
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        $this->assertEquals($first, $fresh->affectedByDelete($connection, $parent, '0'));
        $this->assertSame([], $connection->getQueryLog());
        $connection->disableQueryLog();
    }

    public function test_graph_is_memoized_until_epoch_changes(): void
    {
        $path = sys_get_temp_dir() . '/normcache-delete-dependencies-' . bin2hex(random_bytes(6)) . '.sqlite';
        touch($path);
        $connection = new SQLiteConnection(
            new \PDO('sqlite:' . $path),
            $path,
            '',
            [
                'name' => 'testing',
                'driver' => 'sqlite',
                'database' => $path,
            ],
        );
        $schema = $connection->getSchemaBuilder();
        $schema->create('parents', function (Blueprint $table): void {
            $table->id();
        });
        $schema->create('children', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('parent_id')->constrained('parents')->cascadeOnDelete();
        });

        try {
            $tables = new TableIdentityResolver;
            $resolver = new DeleteDependencyResolver($tables, app(SchemaCache::class));
            $parent = $tables->resolve($connection, 'parents');

            $this->assertNotNull($parent);
            $this->assertSame(
                ['children'],
                $this->affectedTableNames($resolver, $connection, $parent),
            );

            $schema->create('grandchildren', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('child_id')->constrained('children')->cascadeOnDelete();
            });

            $this->assertSame(
                ['children'],
                $this->affectedTableNames($resolver, $connection, $parent),
            );

            $this->assertSame(
                ['children', 'grandchildren'],
                $this->affectedTableNames($resolver, $connection, $parent, '1'),
            );
        } finally {
            unset($connection);
            unlink($path);
        }
    }

    public function test_failed_introspection_returns_null_and_is_retried(): void
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getConfig')->withNoArgs()->andReturn(['name' => 'testing']);
        $connection->shouldReceive('getConfig')->with('search_path')->andReturn(null);
        $connection->shouldReceive('getConfig')->with('schema')->andReturn(null);
        $connection->shouldReceive('getDriverName')->twice()->andReturn('sqlite');
        $connection->shouldReceive('getDatabaseName')->twice()->andReturn('/tmp/database.sqlite');
        $connection->shouldReceive('getTablePrefix')->times(4)->andReturn('');
        $connection->shouldReceive('getSchemaBuilder')->twice()->andThrow(
            new \RuntimeException('Schema metadata is unavailable.'),
        );
        $parent = TableIdentity::fromParts(
            driver: 'sqlite',
            connection: 'testing',
            database: '/tmp/database.sqlite',
            schema: 'main',
            prefix: '',
            table: 'parents',
        );
        $resolver = new DeleteDependencyResolver(new TableIdentityResolver, app(SchemaCache::class));

        $this->assertNull($resolver->affectedByDelete($connection, $parent, '0'));
        $this->assertNull($resolver->affectedByDelete($connection, $parent, '0'));
    }

    /** @return list<string> */
    private function affectedTableNames(
        DeleteDependencyResolver $resolver,
        Connection $connection,
        TableIdentity $parent,
        string $epoch = '0',
    ): array {
        return array_map(
            static fn(TableIdentity $table): string => $table->table,
            $resolver->affectedByDelete($connection, $parent, $epoch) ?? [],
        );
    }
}
