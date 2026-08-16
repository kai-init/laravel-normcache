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
        private SqlVolatilityScanner $volatility = new SqlVolatilityScanner,
    ) {}

    public function analyze(
        Connection $connection,
        QueryBuilder $query,
    ): DependencyAnalysis {
        $directRoot = $this->tables->resolve($connection, $query->from);
        $dependencies = new DependencyCollection;
        $declarations = $query->dependencies();
        $declaredRoot = null;
        $authoritative = false;
        $unresolved = false;

        foreach ($declarations as $declaration) {
            $identity = $declaration->isTable()
                ? $this->tables->resolve($connection, $declaration->value)
                : $this->modelIdentity($connection, $declaration->value);

            if ($identity === null) {
                $unresolved = true;

                continue;
            }

            if ($declaredRoot === null || $identity->hash < $declaredRoot->hash) {
                $declaredRoot = $identity;
            }

            $authoritative = true;
            $dependencies->add($identity);
        }

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
        $this->walk($connection, $query, $dependencies, $directRoot);

        $bypassReason = null;

        if ($unresolved) {
            $bypassReason = 'unresolvable_declared_dependency';
        } elseif ($dependencies->isOpaque() && !$authoritative) {
            $bypassReason = 'unidentifiable_dependency';
        }

        return new DependencyAnalysis(
            root: $root,
            tables: $dependencies->all(),
            queryScoped: $directRoot === null,
            bypassReason: $bypassReason,
            volatile: $dependencies->isVolatile(),
        );
    }

    private function walk(
        Connection $connection,
        Builder $query,
        DependencyCollection $dependencies,
        ?TableIdentity $resolvedSource = null,
    ): void {
        if (!$dependencies->enter($query)) {
            return;
        }

        if ($resolvedSource !== null) {
            $dependencies->add($resolvedSource);
        } elseif ($query->from instanceof Expression) {
            $this->walkExpression(
                $connection,
                $query,
                $query->from,
                $dependencies,
                source: true,
            );
        } else {
            $this->resolveSource($connection, $query->from, $dependencies);
        }

        foreach ($query->joins ?? [] as $join) {
            if ($join->table instanceof Expression) {
                $this->walkExpression(
                    $connection,
                    $query,
                    $join->table,
                    $dependencies,
                    source: true,
                );
            } else {
                $this->resolveSource($connection, $join->table, $dependencies);
            }

            $this->walkValues($connection, $query, $join->wheres, $dependencies);
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
            $this->walkValues($connection, $query, $values, $dependencies);
        }

        foreach ($query->unions ?? [] as $union) {
            $nested = $union['query'] ?? null;
            $nested = $nested instanceof EloquentBuilder ? $nested->toBase() : $nested;

            if ($nested instanceof Builder) {
                $this->walk($connection, $nested, $dependencies);
            } else {
                $dependencies->markOpaque();
            }
        }
    }

    /** @param array<mixed> $values */
    private function walkValues(
        Connection $connection,
        Builder $query,
        array $values,
        DependencyCollection $dependencies,
    ): void {
        foreach ($values as $value) {
            $value = $value instanceof EloquentBuilder ? $value->toBase() : $value;

            if ($value instanceof Builder) {
                $this->walk($connection, $value, $dependencies);

                continue;
            }

            if ($value instanceof Expression) {
                $this->walkExpression($connection, $query, $value, $dependencies);

                continue;
            }

            if (!is_array($value)) {
                continue;
            }

            if (in_array($value['type'] ?? null, ['raw', 'Raw', 'Expression'], true)) {
                $sql = $value['sql'] ?? $value['column'] ?? null;

                if (is_string($sql)) {
                    $this->scanRaw($sql, $dependencies);
                } else {
                    $dependencies->markOpaque();
                }
            }

            $this->walkValues($connection, $query, array_values($value), $dependencies);
        }
    }

    private function walkExpression(
        Connection $connection,
        Builder $query,
        Expression $expression,
        DependencyCollection $dependencies,
        bool $source = false,
    ): void {
        $subquery = $query instanceof QueryBuilder
            ? $query->capturedSubquery($expression)
            : null;

        if ($subquery !== null) {
            $this->walk($connection, $subquery, $dependencies);

            return;
        }

        $sql = (string) $expression->getValue($query->getGrammar());

        // Declared dependencies suppress opacity, not volatility.
        if ($source) {
            $dependencies->markOpaque();
        }

        $this->scanRaw($sql, $dependencies);
    }

    // Scans raw fragments so function-like identifiers remain cacheable.
    private function scanRaw(string $sql, DependencyCollection $dependencies): void
    {
        if ($this->rawMayReferenceSource($sql)) {
            $dependencies->markOpaque();
        }

        if ($this->volatility->isVolatile($sql)) {
            $dependencies->markVolatile();
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
    ): void {
        $identity = $this->tables->resolve($connection, $source);

        if ($identity === null) {
            $dependencies->markOpaque();

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
