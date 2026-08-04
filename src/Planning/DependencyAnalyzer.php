<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
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
        TableIdentity $root,
    ): DependencyAnalysis {
        $resolved = [$root->hash => $root];
        $visited = [];
        $opaque = false;

        $this->walk(
            $connection,
            $query,
            $resolved,
            $visited,
            $opaque,
        );

        $declarations = $query->dependencies();
        $explicit = $declarations !== [];
        $unresolved = false;

        foreach ($declarations as $declaration) {
            $identity = $declaration->isTable()
                ? $this->tables->resolve($connection, $declaration->value)
                : $this->modelIdentity($connection, $declaration->value);

            if ($identity === null) {
                $opaque = true;
                $unresolved = true;

                continue;
            }

            $resolved[$identity->hash] = $identity;
        }

        ksort($resolved, SORT_STRING);

        return new DependencyAnalysis(
            array_values($resolved),
            $opaque,
            $explicit,
            $unresolved,
        );
    }

    /** @param class-string $modelClass */
    public function modelIdentity(Connection $activeConnection, string $modelClass): ?TableIdentity
    {
        try {
            $model = new $modelClass;
            $model->setConnection($activeConnection->getName());

            return $this->tables->resolve($activeConnection, $model->getTable());
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * @param  array<string, TableIdentity>  $resolved
     * @param  array<int, true>  $visited
     */
    private function walk(
        Connection $connection,
        Builder $query,
        array &$resolved,
        array &$visited,
        bool &$opaque,
    ): void {
        $id = spl_object_id($query);

        if (isset($visited[$id])) {
            return;
        }

        $visited[$id] = true;
        $captured = [];
        $this->walkProjectionValues($query, [$query->from], $captured, $opaque);
        $this->resolveSource($connection, $query->from, $resolved, $opaque);

        foreach ($query->joins ?? [] as $join) {
            $this->walkProjectionValues($query, [$join->table], $captured, $opaque);
            $this->resolveSource($connection, $join->table, $resolved, $opaque);
            $this->walkNestedValues(
                $connection,
                $join->wheres,
                $resolved,
                $visited,
                $opaque,
            );
        }

        $this->walkNestedValues(
            $connection,
            $query->wheres,
            $resolved,
            $visited,
            $opaque,
        );
        $this->walkNestedValues(
            $connection,
            $query->havings ?? [],
            $resolved,
            $visited,
            $opaque,
        );
        $this->walkProjectionValues($query, $query->columns ?? [], $captured, $opaque);
        $this->walkProjectionValues(
            $query,
            (array) ($query->aggregate['columns'] ?? []),
            $captured,
            $opaque,
        );
        $this->walkOpaqueValues($query->groups ?? [], $opaque);
        $this->walkOpaqueValues($query->orders ?? [], $opaque);
        $this->walkOpaqueValues($query->unionOrders ?? [], $opaque);

        foreach ($query->unions ?? [] as $union) {
            $nested = $union['query'] ?? null;

            if ($nested instanceof Builder) {
                $this->walk(
                    $connection,
                    $nested,
                    $resolved,
                    $visited,
                    $opaque,
                );
            } else {
                $opaque = true;
            }
        }

        foreach ($captured as $subquery) {
            $this->walk(
                $connection,
                $subquery,
                $resolved,
                $visited,
                $opaque,
            );
        }
    }

    /** @param array<mixed> $values */
    private function walkOpaqueValues(
        array $values,
        bool &$opaque,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                $opaque = true;

                continue;
            }

            if (is_array($value)) {
                if (in_array($value['type'] ?? null, ['raw', 'Raw', 'Expression'], true)) {
                    $opaque = true;
                }

                $this->walkOpaqueValues($value, $opaque);
            }
        }
    }

    /**
     * @param  array<mixed>  $values
     * @param  list<Builder>  $captured  Subqueries this projection stands in for,
     *                                   drained by the caller once the walk completes.
     */
    private function walkProjectionValues(
        Builder $query,
        array $values,
        array &$captured,
        bool &$opaque,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                $subquery = $query instanceof QueryBuilder
                    ? $query->capturedSubquery($value)
                    : null;

                if ($subquery !== null) {
                    $captured[] = $subquery;

                    continue;
                }

                $sql = (string) $value->getValue($query->getGrammar());

                if (preg_match('/\b(?:select|from|join)\b/i', $sql) === 1) {
                    $opaque = true;
                }

                continue;
            }

            if (is_array($value)) {
                $this->walkProjectionValues($query, $value, $captured, $opaque);
            }
        }
    }

    /**
     * @param  array<mixed>  $values
     * @param  array<string, TableIdentity>  $resolved
     * @param  array<int, true>  $visited
     */
    private function walkNestedValues(
        Connection $connection,
        array $values,
        array &$resolved,
        array &$visited,
        bool &$opaque,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Builder) {
                $this->walk(
                    $connection,
                    $value,
                    $resolved,
                    $visited,
                    $opaque,
                );

                continue;
            }

            if ($value instanceof Expression) {
                $opaque = true;

                continue;
            }

            if (!is_array($value)) {
                continue;
            }

            if (in_array($value['type'] ?? null, ['raw', 'Raw', 'Expression'], true)) {
                $opaque = true;
            }

            $this->walkNestedValues(
                $connection,
                array_values($value),
                $resolved,
                $visited,
                $opaque,
            );
        }
    }

    /** @param array<string, TableIdentity> $resolved */
    private function resolveSource(
        Connection $connection,
        mixed $source,
        array &$resolved,
        bool &$opaque,
    ): void {
        $identity = $this->tables->resolve($connection, $source);

        if ($identity === null) {
            $opaque = true;

            return;
        }

        $resolved[$identity->hash] = $identity;
    }
}
