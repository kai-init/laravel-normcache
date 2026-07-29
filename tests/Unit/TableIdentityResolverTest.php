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
        $connection->shouldNotReceive('getConfig');
        $connection->shouldReceive('getSchemaBuilder')->andReturn($builder);

        return $connection;
    }
}
