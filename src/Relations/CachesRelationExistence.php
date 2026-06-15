<?php

namespace NormCache\Relations;

use Closure;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasOneOrMany;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use NormCache\CacheableBuilder;
use NormCache\Values\DependencySet;

/**
 * @mixin CacheableBuilder
 */
trait CachesRelationExistence
{
    private int $totalHasCalls = 0;

    private int $simpleHasCalls = 0;

    private array $existenceDependencies = [];

    private array $existenceTableDependencies = [];

    public function has($relation, $operator = '>=', $count = 1, $boolean = 'and', ?Closure $callback = null): static
    {
        // whereHasMorph/nested has('a.b') run their inner has() calls on a cloned
        // builder, so they never touch this trait's state — the outer call sees
        // no dependency and bypasses safely.
        $this->totalHasCalls++;

        if (is_string($relation) && !str_contains($relation, '.')) {
            $entry = $this->classifyExistenceRelation($relation, $callback);

            if ($entry !== null) {
                $this->simpleHasCalls++;
                $this->existenceDependencies[] = $entry['relatedClass'];

                if ($entry['throughClass'] ?? null) {
                    $this->existenceDependencies[] = $entry['throughClass'];
                }

                if ($entry['tableKey'] ?? null) {
                    $this->existenceTableDependencies[] = $entry['tableKey'];
                }

                array_push($this->existenceDependencies, ...$entry['constraintModels']);
                array_push($this->existenceTableDependencies, ...$entry['constraintTables']);
            }
        }

        return parent::has($relation, $operator, $count, $boolean, $callback);
    }

    public function inferExistenceDependencies(): DependencySet
    {
        if ($this->totalHasCalls === 0) {
            return DependencySet::empty();
        }

        if ($this->totalHasCalls !== $this->simpleHasCalls) {
            return DependencySet::unsafe('whereHas/has dependencies could not be fully inferred.');
        }

        return new DependencySet(
            models: array_values(array_unique($this->existenceDependencies)),
            tables: array_values(array_unique($this->existenceTableDependencies)),
        );
    }

    private function classifyExistenceRelation(string $name, ?callable $constraint): ?array
    {
        $relation = $this->getRelationWithoutConstraints($name);

        if ($relation instanceof MorphTo) {
            return null;
        }

        if (!($relation instanceof HasOneOrMany
            || $relation instanceof BelongsTo
            || $relation instanceof BelongsToMany
            || $relation instanceof HasOneOrManyThrough)) {
            return null;
        }

        return $this->classifyRelationDependency($relation, $constraint);
    }
}
