<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use NormCache\Planning\ConnectionSourceResolver;
use NormCache\Tests\UnitTestCase;

final class ConnectionSourceResolverTest extends UnitTestCase
{
    public function test_explicit_scope_overrides_the_connection_name(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())
            ->method('getConfig')
            ->with(null)
            ->willReturn([
                'name' => 'testing',
                'normcache_scope' => 'shared-source',
            ]);

        $this->assertSame('shared-source', ConnectionSourceResolver::resolve($connection));
    }

    public function test_connection_name_is_the_default_scope(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())
            ->method('getConfig')
            ->with(null)
            ->willReturn(['name' => 'testing']);
        $connection->expects($this->once())
            ->method('getName')
            ->willReturn('testing');

        $this->assertSame('testing', ConnectionSourceResolver::resolve($connection));
    }

    public function test_invalid_or_empty_scopes_are_unidentifiable(): void
    {
        $invalid = $this->createStub(Connection::class);
        $invalid->method('getConfig')->willReturn([
            'name' => 'testing',
            'normcache_scope' => [],
        ]);
        $unnamed = $this->createStub(Connection::class);
        $unnamed->method('getConfig')->willReturn([]);

        $this->assertNull(ConnectionSourceResolver::resolve($invalid));
        $this->assertNull(ConnectionSourceResolver::resolve($unnamed));
    }
}
