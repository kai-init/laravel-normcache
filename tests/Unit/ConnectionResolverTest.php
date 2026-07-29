<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use NormCache\CacheServiceProvider;
use NormCache\Database\Connections\MariaDbConnection;
use NormCache\Database\Connections\MySqlConnection;
use NormCache\Database\Connections\PostgresConnection;
use NormCache\Database\Connections\SQLiteConnection;
use NormCache\Database\Connections\SqlServerConnection;
use NormCache\Tests\UnitTestCase;
use Psr\Log\LoggerInterface;

final class ConnectionResolverTest extends UnitTestCase
{
    public function test_all_supported_drivers_have_resolvers_registered(): void
    {
        $expectedMap = [
            'mysql' => MySqlConnection::class,
            'mariadb' => MariaDbConnection::class,
            'pgsql' => PostgresConnection::class,
            'sqlite' => SQLiteConnection::class,
            'sqlsrv' => SqlServerConnection::class,
        ];

        foreach ($expectedMap as $driver => $expectedClass) {
            $resolver = Connection::getResolver($driver);
            $this->assertNotNull($resolver, "Expected resolver to be registered for {$driver}");

            $connection = $resolver(null, 'test_db', '', ['name' => $driver]);
            $this->assertInstanceOf($expectedClass, $connection);
        }
    }

    public function test_existing_resolver_is_preserved_and_warning_is_logged(): void
    {
        $originalResolver = Connection::getResolver('mysql');
        $customResolver = static fn() => null;

        try {
            Connection::resolverFor('mysql', $customResolver);

            $logger = $this->createMock(LoggerInterface::class);
            $logger->expects($this->once())
                ->method('warning')
                ->with(
                    'NormCache did not replace an existing database connection resolver.',
                    ['driver' => 'mysql'],
                );

            $this->app->instance(LoggerInterface::class, $logger);

            $provider = new CacheServiceProvider($this->app);
            $provider->register();

            $this->assertSame($customResolver, Connection::getResolver('mysql'));
        } finally {
            Connection::resolverFor('mysql', $originalResolver);
        }
    }
}
