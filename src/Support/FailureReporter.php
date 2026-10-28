<?php

namespace NormCache\Support;

use NormCache\Enums\MutationType;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class FailureReporter
{
    private const EVENTS = [
        'invalidation_failed' => [LogLevel::CRITICAL, 'Invalidation failed; cached reads may be stale.'],
        'opaque_write_global_invalidation' => [LogLevel::WARNING, 'Globally invalidated after an intercepted write target could not be resolved.'],
        'delete_dependency_global_invalidation' => [LogLevel::WARNING, 'Globally invalidated because delete dependencies could not be resolved.'],
        'global_invalidation_failed' => [LogLevel::CRITICAL, 'Global invalidation failed; cached reads may be stale.'],
        'invalidation_disable_failed' => [LogLevel::CRITICAL, 'Could not disable cached reads across processes after an invalidation failure.'],
        'observation_failed' => [LogLevel::WARNING, 'Diagnostics failed; the query itself was unaffected.'],
        'repair_unreachable' => [LogLevel::WARNING, 'Could not reload rows from the database to repair a membership.'],
    ];

    /** @var array<string, true> */
    private array $recorded = [];

    public function __construct(private readonly LoggerInterface $logger) {}

    public function cacheUnavailable(\Throwable $exception): void
    {
        if (!$this->claim('cache_unavailable', $exception)) {
            return;
        }

        try {
            report($exception);
        } catch (\Throwable) {
        }
    }

    /** @param list<array{table: TableIdentity, mode: string, token_count: int}> $affected */
    public function invalidationFailed(\Throwable $exception, array $affected): void
    {
        $tables = array_map(fn(array $entry): array => [
            ...$this->tableContext($entry['table']),
            'mode' => $entry['mode'],
            'token_count' => $entry['token_count'],
        ], $affected);

        $this->log('invalidation_failed', count($tables) === 1 ? $tables[0] : ['tables' => $tables], $exception);
    }

    public function opaqueWriteGlobalInvalidation(string $connection): void
    {
        $this->log('opaque_write_global_invalidation', ['connection' => $connection]);
    }

    public function deleteDependencyGlobalInvalidation(TableIdentity $table, MutationType $mutation): void
    {
        $this->log('delete_dependency_global_invalidation', [
            ...$this->tableContext($table),
            'mutation' => strtolower($mutation->name),
        ]);
    }

    public function globalInvalidationFailed(\Throwable $exception, string $reason): void
    {
        $this->log('global_invalidation_failed', ['reason' => $reason], $exception);
    }

    public function invalidationDisableFailed(\Throwable $exception): void
    {
        $this->log('invalidation_disable_failed', [], $exception);
    }

    public function observationFailed(\Throwable $exception, string $outcome): void
    {
        $this->log('observation_failed', ['outcome' => $outcome], $exception);
    }

    public function repairUnreachable(\Throwable $exception, TableIdentity $table, int $tokenCount): void
    {
        $this->log('repair_unreachable', [
            ...$this->tableContext($table),
            'token_count' => $tokenCount,
        ], $exception);
    }

    /** @param array<string, mixed> $context */
    private function log(string $event, array $context, ?\Throwable $exception = null): void
    {
        if (!$this->claim($event, $exception, $context)) {
            return;
        }

        [$level, $message] = self::EVENTS[$event];

        try {
            $this->logger->log($level, $message, [
                'component' => 'normcache',
                'event' => $event,
                ...$context,
                ...($exception === null ? [] : ['exception' => $exception]),
            ]);
        } catch (\Throwable) {
        }
    }

    private function tableContext(TableIdentity $table): array
    {
        return [
            'connection' => $table->connection,
            'table' => $table->qualifiedTable(),
            'table_hash' => $table->hash,
        ];
    }

    /** @param array<string, mixed> $context */
    private function claim(string $event, ?\Throwable $exception, array $context = []): bool
    {
        $fingerprint = serialize([
            $event,
            $exception === null ? null : [$exception::class, $exception->getMessage()],
            $context,
        ]);

        if (isset($this->recorded[$fingerprint])) {
            return false;
        }

        return $this->recorded[$fingerprint] = true;
    }
}
