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
    ) {}

    public function begin(): void
    {
        if ($this->enabled()) {
            $this->startedAt = microtime(true);
        }
    }

    private function elapsed(): array
    {
        $endedAt = microtime(true);

        return [$this->startedAt ?? $endedAt, $endedAt];
    }

    public function hit(
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        QueryStatement $statement,
        ?string $reason = null,
    ): void {
        $this->observe(
            ReadOutcome::HIT,
            $query,
            $plan,
            $hash,
            $statement,
            $reason,
        );
    }

    public function repaired(
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        QueryStatement $statement,
        ?string $reason = null,
    ): void {
        $this->observe(
            ReadOutcome::REPAIRED,
            $query,
            $plan,
            $hash,
            $statement,
            $reason,
        );
    }

    public function miss(
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        QueryStatement $statement,
        ?string $reason = null,
    ): void {
        $this->observe(
            ReadOutcome::MISS,
            $query,
            $plan,
            $hash,
            $statement,
            $reason,
        );
    }

    private function observe(
        ReadOutcome $outcome,
        QueryBuilder $query,
        QueryPlan $plan,
        string $hash,
        QueryStatement $statement,
        ?string $reason,
    ): void {
        if (!$this->enabled()) {
            return;
        }

        [$startedAt, $endedAt] = $this->elapsed();

        if (
            $outcome === ReadOutcome::MISS
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
            ReadOutcome::HIT => QueryCacheHit::class,
            ReadOutcome::MISS => QueryCacheMiss::class,
            ReadOutcome::REPAIRED => QueryCacheRepaired::class,
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
        QueryStatement $statement,
        ?QueryPlan $plan = null,
    ): void {
        if (!$this->enabled()) {
            return;
        }

        [$startedAt, $endedAt] = $this->elapsed();

        $record = new ObservationRecord(
            outcome: 'bypass',
            route: $plan === null ? null : $this->route($plan->route),
            tableHash: $plan?->root->hash,
            queryHash: null,
            reason: $reason,
            sql: $statement->sql(),
            bindings: $statement->bindings(),
            modelClass: $query->modelClass(),
            startedAt: $startedAt,
            endedAt: $endedAt,
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
    }

    public function observing(): bool
    {
        return $this->enabled();
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
        if (isset($this->observedCorruptions[$keyHash])) {
            return false;
        }

        $this->observedCorruptions[$keyHash] = true;

        return true;
    }
}
