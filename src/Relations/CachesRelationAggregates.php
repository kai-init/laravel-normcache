<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;

trait CachesRelationAggregates
{
    private bool $aggregateCaching = true;

    private bool $aggregateRequested = false;

    private array $aggregateAliases = [];

    public function withoutAggregateCache(): static
    {
        $this->aggregateCaching = false;
        $this->aggregateAliases = [];

        if ($this->aggregateRequested) {
            $this->addCapturedContextReason('opted_out', 'withoutAggregateCache() was called explicitly');
        }

        return $this;
    }

    public function withAggregate($relations, $column, $function = null): static
    {
        $this->aggregateRequested = true;

        if (!$this->aggregateCaching) {
            $this->addCapturedContextReason('opted_out', 'withoutAggregateCache() was called explicitly');

            return parent::withAggregate($relations, $column, $function);
        }

        $names = [];
        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
            }

            $segments = explode(' ', (string) $name);
            if (count($segments) === 3 && Str::lower($segments[1]) === 'as') {
                $name = $segments[0];
            }

            $names[] = $name;

            if (str_contains($name, '.')) {
                $this->addCapturedContextReason('dependency', 'nested aggregate relation semantics could not be fully verified');
            } else {
                $this->captureRelationSemantics($name);
            }
        }

        if ($function === 'exists') {
            $this->addCapturedContextReason('dependency', 'withExists() compiles its relation subquery to a raw select');
        }

        $result = parent::withAggregate($relations, $column, $function);
        $newColumns = array_slice($result->getQuery()->columns ?? [], -count($names));

        $aliases = [];
        foreach ($newColumns as $i => $col) {
            $aliases[] = $this->resolveAlias($col, $names[$i] ?? null, $function, $column);
        }

        $this->aggregateAliases = array_values(array_unique([...$this->aggregateAliases, ...$aliases]));

        return $result;
    }

    private function resolveAlias(mixed $column, ?string $name, ?string $function, mixed $columnArg): string
    {
        $grammar = $this->getQuery()->getGrammar();
        $sql = $grammar->isExpression($column) ? $grammar->getValue($column) : (string) $column;

        if (preg_match('/\bas\s+([`"\[]?)([A-Za-z0-9_]+)\1\s*$/i', $sql, $m)) {
            return $m[2];
        }

        $lowerFunction = strtolower((string) $function);
        $colValue = $grammar->isExpression($columnArg) ? $grammar->getValue($columnArg) : $columnArg;

        return Str::snake(preg_replace('/[^[:alnum:][:space:]_]/u', '', "{$name} {$lowerFunction} {$colValue}"));
    }

    public function hasAggregateColumns(): bool
    {
        return $this->aggregateAliases !== [];
    }

    public function resultPayloadFromEloquentModels(Collection $models): array
    {
        $payload = [];
        foreach ($models as $model) {
            $attributes = $model->getRawOriginal();
            foreach ($this->aggregateAliases as $alias) {
                $attributes[$alias] = $model->getAttribute($alias);
            }
            $payload[] = $attributes;
        }

        return $payload;
    }
}
