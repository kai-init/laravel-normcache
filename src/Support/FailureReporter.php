<?php

namespace NormCache\Support;

use NormCache\Enums\MutationType;
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
        if (!$this->claim('cache_unavailable', $exception)) {
            return;
        }

        try {
            report($exception);
        } catch (\Throwable) {
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
            'invalidation_failed',
            'Invalidation failed; cached reads may be stale.',
            $exception,
            [
                ...$this->tableContext($table),
                'mode' => $mode,
                'token_count' => count($tokens),
            ],
            [$table->hash, $mode],
        );
    }

    public function opaqueWriteGlobalInvalidation(string $connection): void
    {
        $this->log(
            LogLevel::WARNING,
            'opaque_write_global_invalidation',
            'Globally invalidated after an intercepted write target could not be resolved.',
            null,
            ['connection' => $connection],
            [$connection],
        );
    }

    public function deleteDependencyGlobalInvalidation(
        TableIdentity $table,
        MutationType $mutation,
    ): void {
        $this->log(
            LogLevel::WARNING,
            'delete_dependency_global_invalidation',
            'Globally invalidated because delete dependencies could not be resolved.',
            null,
            [
                ...$this->tableContext($table),
                'mutation' => strtolower($mutation->name),
            ],
            [$table->hash, $mutation->name],
        );
    }

    public function globalInvalidationFailed(\Throwable $exception, string $reason): void
    {
        $this->log(
            LogLevel::CRITICAL,
            'global_invalidation_failed',
            'Global invalidation failed; cached reads may be stale.',
            $exception,
            ['reason' => $reason],
            [$reason],
        );
    }

    public function observationFailed(\Throwable $exception, string $outcome): void
    {
        $this->log(
            LogLevel::WARNING,
            'observation_failed',
            'Diagnostics failed; the query itself was unaffected.',
            $exception,
            ['outcome' => $outcome],
            [$outcome],
        );
    }

    public function repairUnreachable(
        \Throwable $exception,
        TableIdentity $table,
        int $tokenCount,
    ): void {
        $this->log(
            LogLevel::WARNING,
            'repair_unreachable',
            'Could not reload rows from the database to repair a membership.',
            $exception,
            [
                ...$this->tableContext($table),
                'token_count' => $tokenCount,
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

        $context = [
            'component' => 'normcache',
            'event' => $category,
            ...$context,
            ...($exception === null ? [] : ['exception' => $exception]),
        ];

        try {
            $this->logger->log($level, $message, $context);
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
}
