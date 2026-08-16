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
    // A failed operation observes nothing, so its span is never closed. Capping the
    // stack bounds what an unbalanced begin() can retain.
    private const MAX_SPANS = 8;

    /** @var array<string, true> */
    private array $observedCorruptions = [];

    /** @var list<float> */
    private array $spans = [];

    public function __construct(
        private readonly CacheConfig $config,
        private readonly ?DebugBarCollector $sink,
        private readonly FailureReporter $failures,
    ) {}

    private function guard(string $outcome, \Closure $observation): void
    {
        try {
            $observation();
        } catch (\Throwable $exception) {
            $this->failures->observationFailed($exception, $outcome);
        }
    }

    public function begin(): void
    {
        if (!$this->enabled()) {
            return;
        }

        if (count($this->spans) >= self::MAX_SPANS) {
            array_shift($this->spans);
        }

        $this->spans[] = microtime(true);
    }

    /**
     * Closes the innermost span, so a nested observation cannot consume the span
     * of the read enclosing it.
     *
     * @return array{0: float, 1: float}
     */
    private function elapsed(): array
    {
        $endedAt = microtime(true);

        return [array_pop($this->spans) ?? $endedAt, $endedAt];
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
        $this->guard($outcome->value, function () use (
            $outcome, $query, $plan, $hash, $statement, $reason,
        ): void {
            $this->record($outcome, $query, $plan, $hash, $statement, $reason);
        });
    }

    private function record(
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
        $this->guard('bypass', function () use ($query, $reason, $statement, $plan): void {
            $this->recordBypass($query, $reason, $statement, $plan);
        });
    }

    private function recordBypass(
        QueryBuilder $query,
        string $reason,
        QueryStatement $statement,
        ?QueryPlan $plan,
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
        $this->guard('invalidation', function () use ($table, $mode, $tokens): void {
            if (!$this->enabled()) {
                return;
            }

            [$startedAt, $endedAt] = $this->elapsed();

            $this->recordInvalidation($table, $mode, $tokens, $startedAt, $endedAt);
        });
    }

    /**
     * One batched call invalidated every table, so all of them report its span.
     *
     * @param  list<array{table: TableIdentity, mode: string, tokens: list<string>}>  $invalidations
     */
    public function invalidatedMany(array $invalidations): void
    {
        $this->guard('invalidation', function () use ($invalidations): void {
            if (!$this->enabled()) {
                return;
            }

            [$startedAt, $endedAt] = $this->elapsed();

            foreach ($invalidations as $invalidation) {
                $this->recordInvalidation(
                    $invalidation['table'],
                    $invalidation['mode'],
                    $invalidation['tokens'],
                    $startedAt,
                    $endedAt,
                );
            }
        });
    }

    /** @param list<string> $tokens */
    private function recordInvalidation(
        TableIdentity $table,
        string $mode,
        array $tokens,
        float $startedAt,
        float $endedAt,
    ): void {
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
