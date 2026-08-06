<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Builder;
use Mockery;
use NormCache\Cache\CacheRuntime;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\RedisStore;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class SchemaScopeIsolationTest extends TestCase
{
    private string $currentSchema = 'tenant_a';

    public function test_a_configured_search_path_separates_tenant_identities(): void
    {
        $this->currentSchema = 'tenant_a';
        $a = (new TableIdentityResolver($this->repository()))->resolve(
            $this->postgresConnection(['name' => 'testing', 'search_path' => 'tenant_a']),
            'posts',
        );

        $this->currentSchema = 'tenant_b';
        $b = (new TableIdentityResolver($this->repository()))->resolve(
            $this->postgresConnection(['name' => 'testing', 'search_path' => 'tenant_b']),
            'posts',
        );

        $this->assertNotNull($a);
        $this->assertNotNull($b);
        $this->assertNotSame(
            $a->hash,
            $b->hash,
            'connections configured for different schemas must not share an identity',
        );
    }

    public function test_a_new_scope_reintrospects_the_search_path(): void
    {
        $connection = $this->postgresConnection();

        $this->currentSchema = 'tenant_a';
        $first = (new TableIdentityResolver($this->repository()))->resolve($connection, 'posts');

        $this->assertNotNull($first);
        $this->assertSame('tenant_a', $first->schema);

        $this->currentSchema = 'tenant_b';
        $second = (new TableIdentityResolver($this->repository()))->resolve($connection, 'posts');

        $this->assertNotNull($second);
        $this->assertSame('tenant_b', $second->schema);
        $this->assertNotSame($first->hash, $second->hash);
    }

    public function test_an_explicit_scope_separates_runtime_search_path_changes(): void
    {
        $this->currentSchema = 'tenant_a';
        $a = (new TableIdentityResolver($this->repository()))->resolve(
            $this->postgresConnection(['name' => 'testing', 'normcache_scope' => 'tenant_a']),
            'posts',
        );

        $this->currentSchema = 'tenant_b';
        $b = (new TableIdentityResolver($this->repository()))->resolve(
            $this->postgresConnection(['name' => 'testing', 'normcache_scope' => 'tenant_b']),
            'posts',
        );

        $this->assertNotNull($a);
        $this->assertNotNull($b);
        $this->assertNotSame($a->hash, $b->hash);
    }

    public function test_a_runtime_search_path_change_within_one_scope_is_not_detected(): void
    {
        $connection = $this->postgresConnection();
        $resolver = new TableIdentityResolver($this->repository());

        $this->currentSchema = 'tenant_a';
        $first = $resolver->resolve($connection, 'posts');

        $this->currentSchema = 'tenant_b';
        $second = $resolver->resolve($connection, 'posts');

        $this->assertNotNull($first);
        $this->assertNotNull($second);
        $this->assertSame(
            $first->hash,
            $second->hash,
            'the in-scope memo still answers with the schema resolved on first use',
        );
    }

    private function repository(): SchemaRepository
    {
        $config = $this->app->make(CacheConfig::class);
        $store = $this->app->make(RedisStore::class);
        $keys = $this->app->make(CacheKeyBuilder::class);

        return new SchemaRepository(
            $config,
            $store,
            $keys,
            new CacheRuntime($config, $store, $keys, $this->app->make(FailureReporter::class)),
        );
    }

    /** @param array<string, mixed> $config */
    private function postgresConnection(array $config = ['name' => 'testing']): Connection
    {
        $builder = Mockery::mock(Builder::class);
        $builder->shouldReceive('getCurrentSchemaName')
            ->andReturnUsing(fn(): string => $this->currentSchema);
        $builder->shouldReceive('getViews')->andReturn([]);

        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getDriverName')->andReturn('pgsql');
        $connection->shouldReceive('getName')->andReturn('testing');
        $connection->shouldReceive('getDatabaseName')->andReturn('app');
        $connection->shouldReceive('getTablePrefix')->andReturn('');
        $connection->shouldReceive('getConfig')->andReturn($config);
        $connection->shouldReceive('getSchemaBuilder')->andReturn($builder);

        return $connection;
    }
}
