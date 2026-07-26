<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Builder;
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

    private function postgresConnection(
        ?string $schema,
        ?Builder $builder = null,
    ): Connection {
        $builder ??= Mockery::mock(Builder::class);

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
        $connection->shouldReceive('getSchemaBuilder')->andReturn($builder);

        return $connection;
    }
}
