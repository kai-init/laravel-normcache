<?php

namespace NormCache\Support;

use NormCache\Database\QueryBuilder;
use NormCache\Debug\DebugBarCollector;
use NormCache\Enums\CacheReadOutcome;
use NormCache\Events\CacheInvalidated;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Values\CacheConfig;
use NormCache\Values\ObservationRecord;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class Reporter
{
    /** @var array<string, true> */
    private array $reportedCorruptions = [];

    public function __construct(
        private readonly CacheConfig $config,
        private readonly ?DebugBarCollector $sink,
    ) {}

    public function hit(
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        string $sql,
        array $bindings,
        ?string $reason = null,
    ): void {
        $this->reportQuery(
            CacheReadOutcome::HIT,
            $query,
            $plan,
            $hash,
            $sql,
            $bindings,
            $reason,
        );
    }

    public function repaired(
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        string $sql,
        array $bindings,
        ?string $reason = null,
    ): void {
        $this->reportQuery(
            CacheReadOutcome::REPAIRED,
            $query,
            $plan,
            $hash,
            $sql,
            $bindings,
            $reason,
        );
    }

    public function miss(
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        string $sql,
        array $bindings,
        ?string $reason = null,
    ): void {
        $this->reportQuery(
            CacheReadOutcome::MISS,
            $query,
            $plan,
            $hash,
            $sql,
            $bindings,
            $reason,
        );
    }

    private function reportQuery(
        CacheReadOutcome $outcome,
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        string $sql,
        array $bindings,
        ?string $reason,
    ): void {
        if (!$this->enabled()) {
            return;
        }

        if (
            $outcome === CacheReadOutcome::MISS
            && $reason === 'corrupt_payload'
            && !$this->firstCorruption($hash)
        ) {
            return;
        }

        $record = new ObservationRecord(
            outcome: $outcome->value,
            route: $this->route($plan->route),
            tableHash: $plan->root->hash,
            queryHash: $hash,
            reason: $reason,
            sql: $sql,
            bindings: $bindings,
            modelClass: $query->modelClass(),
        );
        $this->sink?->record($record);

        if (!$this->config->dispatchEvents) {
            return;
        }

        $eventClass = match ($outcome) {
            CacheReadOutcome::HIT => QueryCacheHit::class,
            CacheReadOutcome::MISS => QueryCacheMiss::class,
            CacheReadOutcome::REPAIRED => QueryCacheRepaired::class,
        };

        event(new $eventClass(
            $record->route,
            $record->queryHash,
            $record->tableHash,
            $record->sql,
            $record->bindings,
            $record->modelClass,
            $record->reason,
        ));
    }

    public function bypass(
        QueryBuilder $query,
        string $reason,
        string $sql,
        array $bindings,
        ?QueryPlan $plan = null,
        ?string $queryHash = null,
    ): void {
        if (!$this->enabled()) {
            return;
        }

        $record = new ObservationRecord(
            outcome: 'bypass',
            route: $plan === null ? null : $this->route($plan->route),
            tableHash: $plan?->root->hash,
            queryHash: $queryHash,
            reason: $reason,
            sql: $sql,
            bindings: $bindings,
            modelClass: $query->modelClass(),
        );
        $this->sink?->record($record);

        if ($this->config->dispatchEvents) {
            event(new QueryBypassed(
                $record->reason,
                $record->sql,
                $record->bindings,
                $record->modelClass,
                $record->tableHash,
                $record->queryHash,
                $record->route,
            ));
        }
    }

    /** @param list<string> $tokens */
    public function invalidated(TableIdentity $table, string $mode, array $tokens): void
    {
        if (!$this->enabled()) {
            return;
        }

        $record = new ObservationRecord(
            outcome: 'invalidation',
            tableHash: $table->hash,
            invalidationMode: $mode,
            primaryKeyTokens: $tokens,
        );
        $this->sink?->record($record);

        if ($this->config->dispatchEvents) {
            event(new CacheInvalidated($table->hash, $mode, $tokens));
        }
    }

    private function enabled(): bool
    {
        return $this->config->dispatchEvents || $this->sink !== null;
    }

    private function route(string $route): string
    {
        return str_replace('-', '_', $route);
    }

    private function firstCorruption(string $keyHash): bool
    {
        if (isset($this->reportedCorruptions[$keyHash])) {
            return false;
        }

        $this->reportedCorruptions[$keyHash] = true;

        return true;
    }
}
