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
        $logger->expects($this->exactly(5))->method('log');
        $reporter = new FailureReporter($logger);
        $exception = new \RuntimeException('redis is down');

        $posts = [['table' => $this->table('posts'), 'mode' => 'version', 'token_count' => 1]];
        $authors = [['table' => $this->table('authors'), 'mode' => 'generation', 'token_count' => 1]];
        $reporter->invalidationFailed($exception, $posts);
        $reporter->invalidationFailed($exception, $posts);
        $reporter->invalidationFailed($exception, [
            ['table' => $this->table('posts'), 'mode' => 'version', 'token_count' => 2],
        ]);
        $reporter->invalidationFailed($exception, $authors);
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

        (new FailureReporter($logger))->invalidationFailed($exception, [
            ['table' => $table, 'mode' => 'version', 'token_count' => 2],
        ]);
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
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('log')
            ->willThrowException(new \RuntimeException('The log channel is unreachable.'));

        (new FailureReporter($logger))->invalidationFailed(new \RuntimeException('redis is down'), [
            ['table' => $this->table('posts'), 'mode' => 'version', 'token_count' => 1],
        ]);
    }

    public function test_cache_unavailable_survives_a_failing_exception_handler(): void
    {
        $handler = $this->createMock(ExceptionHandler::class);
        $handler->expects($this->once())->method('report')
            ->willThrowException(new \RuntimeException('The exception handler is broken.'));
        $this->app->instance(ExceptionHandler::class, $handler);

        (new FailureReporter($this->createStub(LoggerInterface::class)))->cacheUnavailable(
            new \RuntimeException('redis is down'),
        );
    }
}
