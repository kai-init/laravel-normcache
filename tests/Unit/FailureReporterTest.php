<?php

namespace NormCache\Tests\Unit;

use Illuminate\Contracts\Debug\ExceptionHandler;
use NormCache\Support\FailureReporter;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class FailureReporterTest extends UnitTestCase
{
    public function test_deduplication_is_per_failure_not_per_category(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->exactly(4))->method('log');
        $reporter = new FailureReporter($logger);
        $exception = new \RuntimeException('redis is down');

        $reporter->invalidationFailed($exception, $this->table('posts'), 'version', ['1']);
        $reporter->invalidationFailed($exception, $this->table('posts'), 'version', ['1']);
        $reporter->invalidationFailed($exception, $this->table('authors'), 'generation', ['2']);
        $reporter->observationFailed($exception, 'hit');
        $reporter->observationFailed($exception, 'hit');
        $reporter->observationFailed($exception, 'miss');
    }

    public function test_invalidation_failure_has_stable_structured_context(): void
    {
        $table = $this->table('posts');
        $exception = new \RuntimeException('redis is down');
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('log')
            ->with(
                LogLevel::CRITICAL,
                'Invalidation failed; cached reads may be stale.',
                [
                    'component' => 'normcache',
                    'event' => 'invalidation_failed',
                    'connection' => 'testing',
                    'table' => 'main.posts',
                    'table_hash' => $table->hash,
                    'mode' => 'version',
                    'token_count' => 2,
                    'exception' => $exception,
                ],
            );

        (new FailureReporter($logger))->invalidationFailed($exception, $table, 'version', ['1', '2']);
    }

    private function table(string $name): TableIdentity
    {
        return TableIdentity::fromParts(
            driver: 'sqlite',
            connection: 'testing',
            database: 'app',
            schema: 'main',
            prefix: '',
            table: $name,
        );
    }

    public function test_reporting_survives_an_unreachable_log_channel(): void
    {
        $reporter = new FailureReporter($this->throwingLogger());

        $reporter->invalidationFailed(
            new \RuntimeException('redis is down'),
            $this->table('posts'),
            'version',
            ['1'],
        );

        $this->assertTrue(true, 'reporting returned without propagating the logging failure');
    }

    public function test_cache_unavailable_survives_a_failing_exception_handler(): void
    {
        $this->app->bind(ExceptionHandler::class, static fn(): ExceptionHandler => new class implements ExceptionHandler
        {
            public function report(\Throwable $e): void
            {
                throw new \RuntimeException('The exception handler is broken.');
            }

            public function shouldReport(\Throwable $e): bool
            {
                return true;
            }

            public function render($request, \Throwable $e)
            {
                return null;
            }

            public function renderForConsole($output, \Throwable $e): void {}
        });

        (new FailureReporter($this->throwingLogger()))->cacheUnavailable(new \RuntimeException('redis is down'));

        $this->assertTrue(true, 'cacheUnavailable() returned without propagating the reporting failure');
    }

    private function throwingLogger(): LoggerInterface
    {
        $logger = $this->createStub(LoggerInterface::class);
        $logger->method('log')
            ->willThrowException(new \RuntimeException('The log channel is unreachable.'));

        return $logger;
    }
}
