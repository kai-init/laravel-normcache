<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Connection;
use NormCache\CacheServiceProvider;
use NormCache\Tests\UnitTestCase;
use Psr\Log\LoggerInterface;

final class ConnectionResolverTest extends UnitTestCase
{
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
            $provider->boot();

            $this->assertSame($customResolver, Connection::getResolver('mysql'));
        } finally {
            Connection::resolverFor('mysql', $originalResolver);
        }
    }
}
