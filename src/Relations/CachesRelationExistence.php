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
use NormCache\Values\RelationDependency;

/**
 * @mixin CacheableBuilder
 */
trait CachesRelationExistence
{
    private ?DependencySet $existenceDependencies = null;

    private bool $existenceInferenceFailed = false;

    public function has($relation, $operator = '>=', $count = 1, $boolean = 'and', ?Closure $callback = null): static
    {
        $dependency = is_string($relation) && !str_contains($relation, '.')
            ? $this->classifyExistenceRelation($relation, $callback)
            : null;

        if ($dependency === null) {
            $this->existenceInferenceFailed = true;
        } else {
            $resolved = new DependencySet(
                models: $dependency->modelDependencies(),
                tables: $dependency->tableDependencies(),
            );
            $this->existenceDependencies = ($this->existenceDependencies ?? DependencySet::empty())->merge($resolved);
        }

        return parent::has($relation, $operator, $count, $boolean, $callback);
    }

    public function inferExistenceDependencies(): DependencySet
    {
        return $this->existenceInferenceFailed
            ? DependencySet::unsafe('whereHas/has dependencies could not be fully inferred.')
            : ($this->existenceDependencies ?? DependencySet::empty());
    }

    private function classifyExistenceRelation(string $name, ?callable $constraint): ?RelationDependency
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

        return (new RelationDependencyClassifier)->classify($relation, $constraint);
    }
}
