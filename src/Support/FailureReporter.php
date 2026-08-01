<?php

namespace NormCache\Support;

use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

final class FailureReporter
{
    /** @var array<string, true> */
    private array $recorded = [];

    public function __construct(private readonly LoggerInterface $logger) {}

    /** A cache dependency failed; reads fall through to the database. */
    public function cacheUnavailable(\Throwable $exception): void
    {
        if ($this->claim($exception)) {
            report($exception);
        }
    }

    /**
     * A write committed but its invalidation did not, so reads may now be stale.
     *
     * @param  list<string>  $tokens
     */
    public function invalidationFailed(
        \Throwable $exception,
        TableIdentity $table,
        string $mode,
        array $tokens,
    ): void {
        if (!$this->claim($exception)) {
            return;
        }

        $this->logger->critical('NormCache invalidation failed; cached reads may be stale.', [
            'exception' => $exception,
            'table' => $this->name($table),
            'mode' => $mode,
            'tokens' => $tokens,
        ]);
    }

    /** Repair could not reload rows from the database; the read falls through instead. */
    public function repairUnreachable(
        \Throwable $exception,
        TableIdentity $table,
        int $tokens,
    ): void {
        if (!$this->claim($exception)) {
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

    private function claim(\Throwable $exception): bool
    {
        $fingerprint = $exception::class . ':' . $exception->getMessage();

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
