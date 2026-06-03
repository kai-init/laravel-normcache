<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use NormCache\Facades\NormCache;
use NormCache\Planning\CachePlanContext;
use NormCache\Planning\DependencySet;
use NormCache\Traits\Cacheable;

trait CachesRelationAggregates
{
    private bool $cacheAggregates = true;

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

        $lowerFunction = strtolower((string) $function);
        $colValue = $this->getQuery()->getGrammar()->isExpression($column)
            ? $this->getQuery()->getGrammar()->getValue($column)
            : $column;

        $dependencies = [];
        $tableDependencies = [];
        $aliases = [];

        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
                $constraint = null;
            }

            $explicitAlias = null;
            $segments = explode(' ', (string) $name);
            if (count($segments) === 3 && Str::lower($segments[1]) === 'as') {
                [$name, $explicitAlias] = [$segments[0], $segments[2]];
            }

            $alias = $explicitAlias ?? Str::snake(
                preg_replace('/[^[:alnum:][:space:]_]/u', '', "{$name} {$lowerFunction} {$colValue}")
            );

            $aliases[] = $alias;

            $entry = $this->classifyAggregate($name, $constraint);

            if ($entry === null) {
                $result = parent::withAggregate($relations, $column, $function);
                $this->clearAggregateTracking();

                return $result;
            }

            $dependencies[] = $entry['relatedClass'];

            if ($entry['throughClass'] ?? null) {
                $dependencies[] = $entry['throughClass'];
            }

            if ($entry['tableKey'] ?? null) {
                $tableDependencies[] = $entry['tableKey'];
            }
        }

        $result = parent::withAggregate($relations, $column, $function);

        $this->aggregateDependencies = array_values(array_unique([...$this->aggregateDependencies, ...$dependencies]));
        $this->aggregateTableDependencies = array_values(array_unique([...$this->aggregateTableDependencies, ...$tableDependencies]));
        $this->aggregateAliases = array_values(array_unique([...$this->aggregateAliases, ...$aliases]));

        return $result;
    }

    private function clearAggregateTracking(): void
    {
        $this->cacheAggregates = false;
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

        $relation = $this->model->{$name}();
        $relatedClass = $relation->getRelated()::class;

        if (!self::relatedIsCacheable($relatedClass)) {
            return null;
        }

        if ($constraint !== null) {
            try {
                $testBuilder = ($relatedClass)::query();
                $constraint($testBuilder);
                $plan = $testBuilder->cachePlan($testBuilder->toBase(), CachePlanContext::models());

                if (
                    !$plan->dependencies->safe
                    || $plan->hasBypassReason('dependency')
                    || $plan->hasBypassReason('normalization')
                ) {
                    return null;
                }
            } catch (\Throwable) {
                return null;
            }
        }

        $throughClass = null;
        if ($relation instanceof HasOneOrManyThrough) {
            $through = (new \ReflectionProperty($relation, 'throughParent'))->getValue($relation)::class;
            if (self::relatedIsCacheable($through)) {
                $throughClass = $through;
            }
        }

        $tableKey = null;
        if ($relation instanceof BelongsToMany) {
            $tableKey = NormCache::tableKey(
                $this->model->getConnection()->getName(),
                $relation->getTable(),
            );
        }

        return [
            'relatedClass' => $relatedClass,
            'throughClass' => $throughClass,
            'tableKey' => $tableKey,
        ];
    }

    public function inferAggregateDependencies(): DependencySet
    {
        if (!$this->cacheAggregates) {
            return DependencySet::unsafe('Aggregate dependencies could not be inferred.');
        }

        return new DependencySet(
            models: $this->aggregateDependencies,
            tables: $this->aggregateTableDependencies,
        );
    }

    private static function relatedIsCacheable(string $class): bool
    {
        static $cache = [];

        return $cache[$class] ??= in_array(Cacheable::class, class_uses_recursive($class), true);
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
