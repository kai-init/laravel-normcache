<?php

namespace NormCache\Values;

use Throwable;

final class RuntimeState
{
    private bool $cacheAvailable = true;

    private ?string $epoch = null;

    private ?bool $runtimeDisabled = null;

    /** @var array<string, true> */
    private array $reportedFailures = [];

    /** @var array<string, true> */
    private array $reportedCorruptions = [];

    /** @var array<string, array<string, array{table: TableIdentity, broad: bool, tokens: array<string, true>}>> */
    private array $pendingInvalidations = [];

    public function available(): bool
    {
        return $this->cacheAvailable;
    }

    /**
     * Read once per scope, so another process's flushAll() is observed by the next
     * request/job rather than mid-scope. The paired read also seeds the kill switch,
     * since both values arrive from one MGET.
     *
     * @param  callable(): array{0: string, 1: bool}  $read
     */
    public function epoch(callable $read): string
    {
        return $this->state($read)[0];
    }

    /**
     * @param  callable(): array{0: string, 1: bool}  $read
     * @return array{0: string, 1: bool}
     */
    public function state(callable $read): array
    {
        if ($this->epoch === null) {
            [$epoch, $disabled] = $read();
            $this->epoch = $epoch;
            $this->runtimeDisabled ??= $disabled;
        }

        return [$this->epoch, $this->runtimeDisabled ?? false];
    }

    /**
     * Deliberately does NOT seed the epoch. Write paths ask this question but never need
     * the epoch, and seeding it from a write would pin an epoch for the rest of the scope
     * that a later cache read then treats as current.
     *
     * @param  callable(): bool  $read
     */
    public function runtimeDisabled(callable $read): bool
    {
        return $this->runtimeDisabled ??= $read();
    }

    public function knownEpoch(): ?string
    {
        return $this->epoch;
    }

    public function rememberEpoch(string $epoch): void
    {
        $this->epoch ??= $epoch;
    }

    public function forgetEpoch(): void
    {
        $this->epoch = null;
        $this->runtimeDisabled = null;
    }

    public function disable(): void
    {
        $this->cacheAvailable = false;
    }

    public function fail(Throwable $exception): void
    {
        $this->disable();

        $fingerprint = $exception::class . ':' . $exception->getMessage();

        if (isset($this->reportedFailures[$fingerprint])) {
            return;
        }

        $this->reportedFailures[$fingerprint] = true;
        report($exception);
    }

    public function firstCorruption(string $keyHash): bool
    {
        if (isset($this->reportedCorruptions[$keyHash])) {
            return false;
        }

        $this->reportedCorruptions[$keyHash] = true;

        return true;
    }

    /** @param list<string> $tokens */
    public function queueInvalidation(TableIdentity $table, bool $broad, array $tokens = []): void
    {
        $current = $this->pendingInvalidations[$table->connection][$table->hash] ?? null;
        $tokenSet = $current['tokens'] ?? [];

        foreach ($tokens as $token) {
            $tokenSet[$token] = true;
        }

        $this->pendingInvalidations[$table->connection][$table->hash] = [
            'table' => $table,
            'broad' => $broad || ($current['broad'] ?? false),
            'tokens' => $tokenSet,
        ];
    }

    /** @return list<array{table: TableIdentity, broad: bool, tokens: array<string, true>}> */
    public function pullInvalidations(string $connection): array
    {
        $pending = array_values($this->pendingInvalidations[$connection] ?? []);
        unset($this->pendingInvalidations[$connection]);

        return $pending;
    }

    public function discardInvalidations(string $connection): void
    {
        unset($this->pendingInvalidations[$connection]);
    }
}
