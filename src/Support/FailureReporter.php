<?php

namespace NormCache\Support;

use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

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
        if (!$this->claim('invalidation', $exception, $table->hash, $mode)) {
            return;
        }

        $this->logger->critical('NormCache invalidation failed; cached reads may be stale.', [
            'exception' => $exception,
            'table' => $this->name($table),
            'mode' => $mode,
            'tokens' => $tokens,
        ]);
    }

    public function repairUnreachable(
        \Throwable $exception,
        TableIdentity $table,
        int $tokens,
    ): void {
        if (!$this->claim('repair', $exception, $table->hash)) {
            return;
        }

        $this->logger->warning(
            'NormCache could not reload rows from the database to repair a membership.',
            [
                'exception' => $exception,
                'table' => $this->name($table),
                'tokens' => $tokens,
            ],
        );
    }

    private function claim(string $category, \Throwable $exception, string ...$context): bool
    {
        $fingerprint = implode('|', [
            $category,
            $exception::class,
            $exception->getMessage(),
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
