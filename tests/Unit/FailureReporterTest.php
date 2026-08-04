<?php

namespace NormCache\Tests\Unit;

use NormCache\Exceptions\CascadeException;
use NormCache\Support\FailureReporter;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class FailureReporterTest extends UnitTestCase
{
    public function test_cascade_failure_owns_its_structured_context(): void
    {
        $original = new \RuntimeException('permission denied');
        $failure = $this->cascadeFailure($original);

        $this->assertSame([
            'stage' => 'foreign_key_listing',
            'driver' => 'pgsql',
            'connection' => 'testing',
            'child' => 'public.children',
            'parent' => 'public.parents',
            'action' => 'mystery',
            'exception' => $original,
        ], $failure->context());
        $this->assertSame([
            'foreign_key_listing',
            'pgsql',
            'testing',
            'public.children',
            'public.parents',
            'mystery',
        ], $failure->fingerprint());
    }

    public function test_cascade_failure_is_logged_once_with_domain_context(): void
    {
        $original = new \RuntimeException('permission denied');
        $failure = $this->cascadeFailure($original);
        $table = TableIdentity::fromParts(
            driver: 'pgsql',
            connection: 'testing',
            database: 'app',
            schema: 'public',
            prefix: '',
            table: 'parents',
        );
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('log')
            ->with(
                LogLevel::WARNING,
                'NormCache globally invalidated because database cascade metadata could not be resolved.',
                [
                    'table' => 'testing:public.parents',
                    ...$failure->context(),
                ],
            );
        $reporter = new FailureReporter($logger);

        $reporter->cascadeGlobalInvalidation($table, $failure);
        $reporter->cascadeGlobalInvalidation($table, $failure);
    }

    private function cascadeFailure(\Throwable $previous): CascadeException
    {
        return new CascadeException(
            stage: 'foreign_key_listing',
            driver: 'pgsql',
            connection: 'testing',
            childReference: 'public.children',
            parentReference: 'public.parents',
            unexpectedAction: 'mystery',
            previous: $previous,
        );
    }
}
