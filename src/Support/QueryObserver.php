<?php

namespace NormCache\Support;

use NormCache\Database\QueryBuilder;
use NormCache\Database\QueryStatement;
use NormCache\Debug\DebugBarCollector;
use NormCache\Enums\ReadOutcome;
use NormCache\Events\CacheInvalidated;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Values\CacheConfig;
use NormCache\Values\ObservationRecord;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryObserver
{
    /** @var array<string, true> */
    private array $observedCorruptions = [];

    private ?float $startedAt = null;

    public function __construct(
        private readonly CacheConfig $config,
        private readonly ?DebugBarCollector $sink,
        private readonly FailureReporter $failures,
    ) {}

    public function begin(): void
    {
        if ($this->observing()) {
            $this->startedAt = microtime(true);
        }
    }

    private function elapsed(): array
    {
        $endedAt = microtime(true);

        return [$this->startedAt ?? $endedAt, $endedAt];
    }

    public function read(
        ReadOutcome $outcome,
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        QueryStatement $statement,
        ?string $reason = null,
    ): void {
        $this->recordQuery($outcome->value, $query, $plan, $hash, $statement, $reason);
    }

    private function recordQuery(
        string $outcome,
        QueryBuilder $query,
        ?QueryPlan $plan,
        ?string $hash,
        QueryStatement $statement,
        ?string $reason,
    ): void {
        if (!$this->observing()) {
            return;
        }

        try {
            [$startedAt, $endedAt] = $this->elapsed();

            if (
                $outcome === 'miss'
                && $reason === 'corrupt_payload'
                && !$this->firstCorruption((string) $hash)
            ) {
                return;
            }

            $route = $plan === null ? null : str_replace('-', '_', $plan->route);
            $record = new ObservationRecord(
                outcome: $outcome,
                route: $route,
                tableHash: $plan?->root->hash,
                queryHash: $hash,
                reason: $reason,
                sql: $statement->sql(),
                bindings: $statement->bindings(),
                modelClass: $query->modelClass(),
                startedAt: $startedAt,
                endedAt: $endedAt,
            );
            $this->sink?->record($record);

            if (!$this->config->dispatchEvents) {
                return;
            }

            $eventClass = match ($outcome) {
                'hit' => QueryCacheHit::class,
                'miss' => QueryCacheMiss::class,
                'repair' => QueryCacheRepaired::class,
                'bypass' => QueryBypassed::class,
            };

            event(new $eventClass(
                route: $record->route,
                queryHash: $record->queryHash,
                tableHash: $record->tableHash,
                sql: $record->sql,
                bindings: $record->bindings,
                modelClass: $record->modelClass,
                reason: $record->reason,
            ));
        } catch (\Throwable $exception) {
            $this->failures->observationFailed($exception, $outcome);
        }
    }

    public function bypass(
        QueryBuilder $query,
        string $reason,
        QueryStatement $statement,
        ?QueryPlan $plan = null,
    ): void {
        $this->recordQuery('bypass', $query, $plan, null, $statement, $reason);
    }

    /** @param list<string> $tokens */
    public function invalidated(TableIdentity $table, string $mode, array $tokens): void
    {
        if (!$this->observing()) {
            return;
        }

        try {
            [$startedAt, $endedAt] = $this->elapsed();
            $record = new ObservationRecord(
                outcome: 'invalidation',
                tableHash: $table->hash,
                invalidationMode: $mode,
                primaryKeyTokens: $tokens,
                startedAt: $startedAt,
                endedAt: $endedAt,
            );
            $this->sink?->record($record);

            if ($this->config->dispatchEvents) {
                event(new CacheInvalidated($table->hash, $mode, $tokens));
            }
        } catch (\Throwable $exception) {
            $this->failures->observationFailed($exception, 'invalidation');
        }
    }

    public function observing(): bool
    {
        return $this->config->dispatchEvents || $this->sink !== null;
    }

    private function firstCorruption(string $keyHash): bool
    {
        if (isset($this->observedCorruptions[$keyHash])) {
            return false;
        }

        $this->observedCorruptions[$keyHash] = true;

        return true;
    }
}
