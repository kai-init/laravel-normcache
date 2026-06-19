<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use NormCache\Values\DependencySet;

trait CachesRelationAggregates
{
    private bool $cacheAggregates = true;

    private bool $aggregateInferenceFailed = false;

    private array $aggregateDependencies = [];

    private array $aggregateTableDependencies = [];

    private array $aggregateAliases = [];

    public function withoutAggregateCache(): static
    {
        $this->clearAggregateTracking();

        return $this;
    }

    public function withAggregate($relations, $column, $function = null): static
    {
        if (!$this->cacheAggregates) {
            return parent::withAggregate($relations, $column, $function);
        }

        $dependencies = [];
        $tableDependencies = [];
        $names = [];

        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
                $constraint = null;
            }

            $segments = explode(' ', (string) $name);
            if (count($segments) === 3 && Str::lower($segments[1]) === 'as') {
                $name = $segments[0];
            }

            $names[] = $name;

            $entry = $this->classifyAggregate($name, $constraint);

            if ($entry === null) {
                $result = parent::withAggregate($relations, $column, $function);
                $this->clearAggregateTracking(true);

                return $result;
            }

            $dependencies[] = $entry['relatedClass'];

            if ($entry['throughClass'] ?? null) {
                $dependencies[] = $entry['throughClass'];
            }

            if ($entry['tableKey'] ?? null) {
                $tableDependencies[] = $entry['tableKey'];
            }

            array_push($dependencies, ...$entry['constraintModels']);
            array_push($tableDependencies, ...$entry['constraintTables']);
        }

        $result = parent::withAggregate($relations, $column, $function);
        // Slice from the end: the first withAggregate() call on a bare query also injects a
        // "table.*" wildcard column, which would shift a from-the-front slice by one.
        $newColumns = array_slice($result->getQuery()->columns ?? [], -count($names));

        $aliases = [];
        foreach ($newColumns as $i => $col) {
            $aliases[] = $this->resolveAlias($col, $names[$i] ?? null, $function, $column);
        }

        $this->aggregateDependencies = array_values(array_unique([...$this->aggregateDependencies, ...$dependencies]));
        $this->aggregateTableDependencies = array_values(array_unique([...$this->aggregateTableDependencies, ...$tableDependencies]));
        $this->aggregateAliases = array_values(array_unique([...$this->aggregateAliases, ...$aliases]));

        return $result;
    }

    // Eloquent assigns this alias internally but exposes no way to read it back; parse it out of
    // the rendered SQL, falling back to predicting it the way Eloquent itself does if unmatched.
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

    private function clearAggregateTracking(bool $failed = false): void
    {
        $this->cacheAggregates = false;
        $this->aggregateInferenceFailed = $failed;
        $this->aggregateDependencies = [];
        $this->aggregateTableDependencies = [];
        $this->aggregateAliases = [];
    }

    private function classifyAggregate(
        string $name,
        ?callable $constraint,
    ): ?array {
        if (str_contains($name, '.')) {
            return null;
        }

        return (new RelationDependencyClassifier)->classify($this->model->{$name}(), $constraint);
    }

    public function inferAggregateDependencies(): DependencySet
    {
        $aggregate = match (true) {
            !$this->cacheAggregates && $this->aggregateInferenceFailed => DependencySet::unsafe('Aggregate dependencies could not be inferred.'),
            !$this->cacheAggregates => DependencySet::empty(),
            $this->aggregateDependencies === [] && $this->aggregateTableDependencies === [] => DependencySet::empty(),
            default => new DependencySet(
                models: $this->aggregateDependencies,
                tables: $this->aggregateTableDependencies,
            ),
        };

        return $aggregate->merge($this->inferExistenceDependencies());
    }

    public function hasAggregateColumns(): bool
    {
        return $this->aggregateAliases !== [];
    }

    public function resultPayloadFromEloquentModels(Collection $models): array
    {
        $aliases = $this->aggregateAliases;

        $payload = [];
        foreach ($models as $model) {
            $attributes = $model->getRawOriginal();
            foreach ($aliases as $alias) {
                $attributes[$alias] = $model->getAttribute($alias);
            }
            $payload[] = $attributes;
        }

        return $payload;
    }
}
