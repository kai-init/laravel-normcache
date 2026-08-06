<?php

namespace NormCache\Support;

use NormCache\Exceptions\CascadeException;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class FailureReporter
{
    /** @var array<string, true> */
    private array $recorded = [];

    public function __construct(private readonly LoggerInterface $logger) {}

    public function cacheUnavailable(\Throwable $exception): void
    {
        if ($this->claim('cache', $exception)) {
            report($exception);
        }
    }

    /** @param  list<string>  $tokens */
    public function invalidationFailed(
        \Throwable $exception,
        TableIdentity $table,
        string $mode,
        array $tokens,
    ): void {
        $this->log(
            LogLevel::CRITICAL,
            'invalidation',
            'NormCache invalidation failed; cached reads may be stale.',
            $exception,
            [
                'exception' => $exception,
                'table' => $this->name($table),
                'mode' => $mode,
                'tokens' => $tokens,
            ],
            [$table->hash, $mode],
        );
    }

    public function opaqueWriteGlobalInvalidation(string $connection): void
    {
        $this->log(
            LogLevel::WARNING,
            'opaque-write',
            'NormCache globally invalidated after an intercepted write target could not be resolved.',
            null,
            ['connection' => $connection],
            [$connection],
        );
    }

    public function cascadeGlobalInvalidation(
        ?TableIdentity $table,
        CascadeException $failure,
    ): void {
        $tableHash = $table === null ? '' : $table->hash;
        $tableName = $table === null ? null : $this->name($table);

        $this->log(
            LogLevel::WARNING,
            'cascade-metadata',
            'NormCache globally invalidated because database cascade metadata could not be resolved.',
            $failure,
            [
                'table' => $tableName,
                ...$failure->context(),
            ],
            [$tableHash, ...$failure->fingerprint()],
        );
    }

    public function globalInvalidationFailed(\Throwable $exception, string $reason): void
    {
        $this->log(
            LogLevel::CRITICAL,
            'global-invalidation',
            'NormCache global invalidation failed; cached reads may be stale.',
            $exception,
            [
                'exception' => $exception,
                'reason' => $reason,
            ],
            [$reason],
        );
    }

    public function observationFailed(\Throwable $exception, string $outcome): void
    {
        $this->log(
            LogLevel::WARNING,
            'observation',
            'NormCache diagnostics failed; the query itself was unaffected.',
            $exception,
            [
                'exception' => $exception,
                'outcome' => $outcome,
            ],
            [$outcome],
        );
    }

    public function repairUnreachable(
        \Throwable $exception,
        TableIdentity $table,
        int $tokens,
    ): void {
        $this->log(
            LogLevel::WARNING,
            'repair',
            'NormCache could not reload rows from the database to repair a membership.',
            $exception,
            [
                'exception' => $exception,
                'table' => $this->name($table),
                'tokens' => $tokens,
            ],
            [$table->hash],
        );
    }

    /**
     * @param  array<string, mixed>  $context
     * @param  list<string>  $fingerprint
     */
    private function log(
        string $level,
        string $category,
        string $message,
        ?\Throwable $exception,
        array $context,
        array $fingerprint = [],
    ): void {
        if (!$this->claim($category, $exception, ...$fingerprint)) {
            return;
        }

        $this->logger->log($level, $message, $context);
    }

    private function claim(string $category, ?\Throwable $exception, string ...$context): bool
    {
        $fingerprint = implode('|', [
            $category,
            ...($exception === null ? [] : [$exception::class, $exception->getMessage()]),
            ...$context,
        ]);

        if (isset($this->recorded[$fingerprint])) {
            return false;
        }

        return $this->recorded[$fingerprint] = true;
    }

    private function name(TableIdentity $table): string
    {
        return $table->connection . ':' . $table->qualifiedTable();
    }
}
