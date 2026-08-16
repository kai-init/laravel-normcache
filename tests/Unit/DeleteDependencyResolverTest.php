<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\SQLiteConnection;
use Mockery;
use NormCache\Planning\DeleteDependencyResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\TableIdentity;

final class DeleteDependencyResolverTest extends UnitTestCase
{
    public function test_process_graph_is_memoized_until_cleared(): void
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
            $resolver = new DeleteDependencyResolver($tables);
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

            $resolver->clear();

            $this->assertSame(
                ['children', 'grandchildren'],
                $this->affectedTableNames($resolver, $connection, $parent),
            );
        } finally {
            unset($connection);
            unlink($path);
        }
    }

    public function test_failed_introspection_returns_null_and_is_retried(): void
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getConfig')->twice()->andReturn(['name' => 'testing']);
        $connection->shouldReceive('getName')->andReturn('testing');
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
        $resolver = new DeleteDependencyResolver(new TableIdentityResolver);

        $this->assertNull($resolver->affectedByDelete($connection, $parent));
        $this->assertNull($resolver->affectedByDelete($connection, $parent));
    }

    /** @return list<string> */
    private function affectedTableNames(
        DeleteDependencyResolver $resolver,
        Connection $connection,
        TableIdentity $parent,
    ): array {
        return array_map(
            static fn(TableIdentity $table): string => $table->table,
            $resolver->affectedByDelete($connection, $parent) ?? [],
        );
    }
}
