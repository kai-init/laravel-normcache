<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Query\Builder;
use NormCache\Database\QueryBuilder;
use NormCache\Values\DependencyAnalysis;
use NormCache\Values\TableIdentity;

final class DependencyAnalyzer
{
    public function __construct(
        private TableIdentityResolver $tables,
    ) {}

    public function analyze(
        Connection $connection,
        QueryBuilder $query,
    ): DependencyAnalysis {
        $directRoot = $this->tables->resolve($connection, $query->from);
        $dependencies = new DependencyCollection;
        $declaredRoot = $this->walk($connection, $query, $dependencies, $directRoot);

        $root = $directRoot
            ?? $this->modelRoot($connection, $query)
            ?? $declaredRoot;

        if ($root === null) {
            return new DependencyAnalysis(
                root: null,
                tables: $dependencies->all(),
                queryScoped: true,
                bypassReason: 'unidentifiable_dependency',
            );
        }

        $dependencies->add($root);

        return new DependencyAnalysis(
            root: $root,
            tables: $dependencies->all(),
            queryScoped: $directRoot === null,
            bypassReason: $dependencies->bypassReason(),
        );
    }

    private function walk(
        Connection $connection,
        Builder $query,
        DependencyCollection $dependencies,
        ?TableIdentity $resolvedSource = null,
        bool $authoritative = false,
    ): ?TableIdentity {
        $declaredRoot = null;

        foreach ($query instanceof QueryBuilder ? $query->dependencies() : [] as $declaration) {
            $identity = $declaration->isTable()
                ? $this->tables->resolve($connection, $declaration->value)
                : $this->modelIdentity($connection, $declaration->value);

            if ($identity === null) {
                $dependencies->markUnresolvedDeclaration();

                continue;
            }

            if ($declaredRoot === null || $identity->hash < $declaredRoot->hash) {
                $declaredRoot = $identity;
            }

            $dependencies->add($identity);
        }

        $authoritative = $authoritative || $declaredRoot !== null;

        if (!$dependencies->enter($query, $authoritative)) {
            return $declaredRoot;
        }

        if ($resolvedSource !== null) {
            $dependencies->add($resolvedSource);
        } elseif ($query->from instanceof Expression) {
            $this->walkExpression(
                $connection,
                $query,
                $query->from,
                $dependencies,
                $authoritative,
                source: true,
            );
        } else {
            $this->resolveSource($connection, $query->from, $dependencies, $authoritative);
        }

        foreach ($query->joins ?? [] as $join) {
            if ($join->table instanceof Expression) {
                $this->walkExpression(
                    $connection,
                    $query,
                    $join->table,
                    $dependencies,
                    $authoritative,
                    source: true,
                );
            } else {
                $this->resolveSource($connection, $join->table, $dependencies, $authoritative);
            }

            $this->walkValues($connection, $query, $join->wheres, $dependencies, $authoritative);
        }

        foreach ([
            $query->wheres,
            $query->havings ?? [],
            $query->columns ?? [],
            (array) ($query->aggregate['columns'] ?? []),
            $query->groups ?? [],
            $query->orders ?? [],
            $query->unionOrders ?? [],
        ] as $values) {
            $this->walkValues($connection, $query, $values, $dependencies, $authoritative);
        }

        foreach ($query->unions ?? [] as $union) {
            $nested = $union['query'] ?? null;
            $nested = $nested instanceof EloquentBuilder ? $nested->toBase() : $nested;

            if ($nested instanceof Builder) {
                $this->walk($connection, $nested, $dependencies, authoritative: $authoritative);
            } elseif (!$authoritative) {
                $dependencies->markOpaque();
            }
        }

        return $declaredRoot;
    }

    /** @param array<mixed> $values */
    private function walkValues(
        Connection $connection,
        Builder $query,
        array $values,
        DependencyCollection $dependencies,
        bool $authoritative,
    ): void {
        foreach ($values as $value) {
            $value = $value instanceof EloquentBuilder ? $value->toBase() : $value;

            if ($value instanceof Builder) {
                $this->walk($connection, $value, $dependencies, authoritative: $authoritative);

                continue;
            }

            if ($value instanceof Expression) {
                $this->walkExpression($connection, $query, $value, $dependencies, $authoritative);

                continue;
            }

            if (!is_array($value)) {
                continue;
            }

            if (in_array($value['type'] ?? null, ['raw', 'Raw', 'Expression'], true)) {
                $sql = $value['sql'] ?? $value['column'] ?? null;

                if (!$authoritative && (!is_string($sql) || $this->rawMayReferenceSource($sql))) {
                    $dependencies->markOpaque();
                }
            }

            $this->walkValues($connection, $query, array_values($value), $dependencies, $authoritative);
        }
    }

    private function walkExpression(
        Connection $connection,
        Builder $query,
        Expression $expression,
        DependencyCollection $dependencies,
        bool $authoritative,
        bool $source = false,
    ): void {
        $subquery = $query instanceof QueryBuilder
            ? $query->capturedSubquery($expression)
            : null;

        if ($subquery !== null) {
            $this->walk($connection, $subquery, $dependencies, authoritative: $authoritative);

            return;
        }

        if ($source) {
            if (!$authoritative) {
                $dependencies->markOpaque();
            }

            return;
        }

        $sql = (string) $expression->getValue($query->getGrammar());

        if (!$authoritative && $this->rawMayReferenceSource($sql)) {
            $dependencies->markOpaque();
        }
    }

    private function rawMayReferenceSource(string $sql): bool
    {
        return preg_match(
            '/(?:--|#|\/\*)|\b(?:select|from|join|table|with|union|intersect|except|using|natural|lateral|apply|only|tablesample)\b/i',
            $sql,
        ) !== 0;
    }

    private function resolveSource(
        Connection $connection,
        mixed $source,
        DependencyCollection $dependencies,
        bool $authoritative,
    ): void {
        $identity = $this->tables->resolve($connection, $source);

        if ($identity === null) {
            if (!$authoritative) {
                $dependencies->markOpaque();
            }

            return;
        }

        $dependencies->add($identity);
    }

    private function modelRoot(
        Connection $connection,
        QueryBuilder $query,
    ): ?TableIdentity {
        $modelClass = $query->modelClass();

        return $modelClass === null
            ? null
            : $this->modelIdentity($connection, $modelClass);
    }

    /** @param class-string $modelClass */
    private function modelIdentity(
        Connection $activeConnection,
        string $modelClass,
    ): ?TableIdentity {
        try {
            $model = new $modelClass;
            $model->setConnection($activeConnection->getName());

            return $this->tables->resolve($activeConnection, $model->getTable());
        } catch (\Throwable) {
            return null;
        }
    }
}
