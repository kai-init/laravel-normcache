<?php

namespace NormCache\Values;

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

    public function runtimeDisabled(callable $read): bool
    {
        if ($this->runtimeDisabled === true) {
            $this->runtimeDisabled = $read();
        } else {
            $this->runtimeDisabled ??= $read();
        }

        return $this->runtimeDisabled;
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

    public function fail(\Throwable $exception): void
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
