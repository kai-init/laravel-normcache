<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use NormCache\Values\DependencySet;
use NormCache\Values\RelationDependency;

trait CachesRelationAggregates
{
    private ?bool $aggregateCaching = true;

    private ?DependencySet $aggregateDependencies = null;

    private array $aggregateAliases = [];

    public function withoutAggregateCache(): static
    {
        $this->clearAggregateTracking();

        return $this;
    }

    public function withAggregate($relations, $column, $function = null): static
    {
        if ($this->aggregateCaching !== true) {
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

            $dependency = $this->classifyAggregate($name, $constraint);

            if ($dependency === null) {
                $result = parent::withAggregate($relations, $column, $function);
                $this->clearAggregateTracking(null);

                return $result;
            }

            array_push($dependencies, ...$dependency->modelDependencies());
            array_push($tableDependencies, ...$dependency->tableDependencies());
        }

        $result = parent::withAggregate($relations, $column, $function);
        // Slice from the end: the first withAggregate() call on a bare query also injects a
        // "table.*" wildcard column, which would shift a from-the-front slice by one.
        $newColumns = array_slice($result->getQuery()->columns ?? [], -count($names));

        $aliases = [];
        foreach ($newColumns as $i => $col) {
            $aliases[] = $this->resolveAlias($col, $names[$i] ?? null, $function, $column);
        }

        $resolved = new DependencySet(
            models: array_values(array_unique($dependencies)),
            tables: array_values(array_unique($tableDependencies)),
        );
        $this->aggregateDependencies = ($this->aggregateDependencies ?? DependencySet::empty())->merge($resolved);
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

    private function clearAggregateTracking(?bool $state = false): void
    {
        $this->aggregateCaching = $state;
        $this->aggregateDependencies = null;
        $this->aggregateAliases = [];
    }

    private function classifyAggregate(
        string $name,
        ?callable $constraint,
    ): ?RelationDependency {
        if (str_contains($name, '.')) {
            return null;
        }

        return (new RelationDependencyClassifier)->classify($this->model->{$name}(), $constraint);
    }

    public function inferRelationDependencies(): DependencySet
    {
        $aggregate = match ($this->aggregateCaching) {
            null => DependencySet::unsafe('Aggregate dependencies could not be inferred.'),
            false => DependencySet::empty(),
            true => $this->aggregateDependencies ?? DependencySet::empty(),
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
