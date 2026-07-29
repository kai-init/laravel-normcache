<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use NormCache\Database\QueryBuilder;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlanner
{
    /** @param list<TableIdentity> $dependencies */
    public function plan(
        QueryBuilder $query,
        TableIdentity $root,
        ?PrimaryKeyMetadata $primaryKey,
        array $dependencies,
        bool $forceQueryGroup = false,
        string $operation = 'select',
    ): QueryPlan {
        $dependencies = $this->uniqueDependencies($dependencies);

        if ($forceQueryGroup || $query->joins !== null && $query->joins !== []) {
            return new QueryPlan(QueryPlan::QUERY_GROUP, $root, $dependencies, $primaryKey);
        }

        if ($this->hasCrossTableUnion($root, $dependencies, $query)) {
            return new QueryPlan(QueryPlan::QUERY_GROUP, $root, $dependencies, $primaryKey);
        }

        $canUseRowShape = $operation === 'select'
            && $primaryKey !== null
            && $this->isSingleRowShape($query);

        if (
            $canUseRowShape
            && count($dependencies) === 1
            && $this->allowsDirectControls($query)
        ) {
            $directToken = $this->directPrimaryKeyToken($query, $root, $primaryKey);

            if ($directToken !== null) {
                [$softDeleteSafe, $softDeleteMode] = $this->softDeleteMode($query);

                if ($softDeleteSafe) {
                    $deletedAtColumn = $query->deletedAtColumn();

                    if ($this->isWildcard($query, $root)) {
                        return new QueryPlan(
                            QueryPlan::DIRECT_PK,
                            $root,
                            $dependencies,
                            $primaryKey,
                            $directToken,
                            softDeleteMode: $softDeleteMode,
                            deletedAtColumn: $deletedAtColumn,
                        );
                    }

                    if (($projectedColumns = $this->plainColumns($query, $root)) !== null) {
                        return new QueryPlan(
                            QueryPlan::RESULT,
                            $root,
                            $dependencies,
                            $primaryKey,
                            $directToken,
                            softDeleteMode: $softDeleteMode,
                            deletedAtColumn: $deletedAtColumn,
                            projectedColumns: $projectedColumns,
                        );
                    }
                }
            }
        }

        if (
            $canUseRowShape
            && $this->isWildcard($query, $root)
            && (!$root->isView || !$this->hasExternalDependency($root, $dependencies))
        ) {
            return new QueryPlan(
                QueryPlan::CANONICAL,
                $root,
                $dependencies,
                $primaryKey,
                materializeResult: $query->usesResultCache(),
            );
        }

        if (
            $canUseRowShape
            && ($projectedColumns = $this->plainColumns($query, $root)) !== null
        ) {
            return new QueryPlan(
                QueryPlan::RESULT,
                $root,
                $dependencies,
                $primaryKey,
                projectedColumns: $projectedColumns,
            );
        }

        return new QueryPlan(QueryPlan::RESULT, $root, $dependencies, $primaryKey);
    }

    private function allowsDirectControls(QueryBuilder $query): bool
    {
        return $query->configuredTag() === null && $query->configuredTtl() === null;
    }

    private function isSingleRowShape(QueryBuilder $query): bool
    {
        return !$query->distinct
            && $query->aggregate === null
            && $query->groupLimit === null
            && empty($query->groups)
            && empty($query->havings)
            && empty($query->unions);
    }

    /** @param list<TableIdentity> $dependencies
     * @return list<TableIdentity>
     */
    private function uniqueDependencies(array $dependencies): array
    {
        if (count($dependencies) <= 1) {
            return $dependencies;
        }

        $unique = [];

        foreach ($dependencies as $dependency) {
            $unique[$dependency->hash] = $dependency;
        }

        ksort($unique, SORT_STRING);

        return array_values($unique);
    }

    private function hasCrossTableUnion(
        TableIdentity $root,
        array $dependencies,
        QueryBuilder $query,
    ): bool {
        return !empty($query->unions)
            && $this->hasExternalDependency($root, $dependencies);
    }

    private function hasExternalDependency(TableIdentity $root, array $dependencies): bool
    {
        return count(array_filter(
            $dependencies,
            static fn(TableIdentity $dependency): bool => $dependency->hash !== $root->hash,
        )) > 0;
    }

    private function isWildcard(QueryBuilder $query, TableIdentity $root): bool
    {
        if ($query->columns === null || $query->columns === ['*']) {
            return true;
        }

        if (count($query->columns) !== 1 || !is_string($query->columns[0])) {
            return false;
        }

        $column = strtolower(trim($query->columns[0]));

        $alias = $this->fromAlias($query);

        return $alias !== null
            ? $column === $alias . '.*'
            : $column === strtolower($root->table) . '.*';
    }

    private function fromAlias(QueryBuilder $query): ?string
    {
        if (
            is_string($query->from)
            && preg_match('/\\s+(?:as\\s+)?([^\\s]+)$/i', trim($query->from), $matches) === 1
        ) {
            return strtolower($matches[1]);
        }

        return null;
    }

    /** @return list<string>|null */
    private function plainColumns(QueryBuilder $query, TableIdentity $root): ?array
    {
        if ($query->columns === null || $query->columns === ['*']) {
            return null;
        }

        $table = strtolower($root->table);
        $alias = $this->fromAlias($query);
        $columns = [];

        foreach ($query->columns as $column) {
            if (!is_string($column)) {
                return null;
            }

            $normalized = trim($column);

            if (preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $normalized) === 1) {
                $columns[] = $normalized;

                continue;
            }

            if (preg_match('/^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$/', $normalized, $matches) !== 1) {
                return null;
            }

            $qualifier = strtolower($matches[1]);

            if (!$this->isValidQualifier($qualifier, $table, $alias)) {
                return null;
            }

            $columns[] = $matches[2];
        }

        return $columns === [] ? null : $columns;
    }

    private function directPrimaryKeyToken(
        QueryBuilder $query,
        TableIdentity $root,
        PrimaryKeyMetadata $primaryKey,
    ): ?string {
        if (
            $query->offset !== null
            || ($query->limit !== null && $query->limit !== 1)
            || !empty($query->orders)
        ) {
            return null;
        }

        $primaryWhere = null;

        foreach ($query->wheres as $where) {
            if ($this->isSoftDeleteWhere($query, $where)) {
                continue;
            }

            if ($primaryWhere !== null) {
                return null;
            }

            $primaryWhere = $where;
        }

        if ($primaryWhere === null) {
            return null;
        }

        $where = $primaryWhere;

        if (
            ($where['type'] ?? null) !== 'Basic'
            || strtolower((string) ($where['boolean'] ?? 'and')) !== 'and'
            || !in_array($where['operator'] ?? null, ['=', '=='], true)
            || !is_string($where['column'] ?? null)
            || $where['value'] instanceof Expression
        ) {
            return null;
        }

        $column = trim((string) $where['column']);
        $parts = explode('.', $column);
        $unqualified = trim((string) array_pop($parts), '`"[]');

        if (strtolower($unqualified) !== strtolower($primaryKey->column)) {
            return null;
        }

        if ($parts !== []) {
            $qualifier = strtolower(trim((string) array_pop($parts), '`"[]'));
            $table = strtolower($root->table);
            $alias = $this->fromAlias($query);

            if ($parts !== [] || !$this->isValidQualifier($qualifier, $table, $alias)) {
                return null;
            }
        }

        return $primaryKey->token($where['value'] ?? null);
    }

    /** @return array{0: bool, 1: ?string} */
    private function softDeleteMode(QueryBuilder $query): array
    {
        if ($query->deletedAtColumn() === null) {
            return [true, null];
        }

        $modes = [];

        foreach ($query->wheres as $where) {
            if (!$this->isDeletedAtColumn($query, $where['column'] ?? null)) {
                continue;
            }

            $type = strtolower((string) ($where['type'] ?? ''));

            if (
                !in_array($type, ['null', 'notnull'], true)
                || strtolower((string) ($where['boolean'] ?? 'and')) !== 'and'
            ) {
                return [false, null];
            }

            $modes[] = $type === 'notnull' ? 'only' : 'default';
        }

        if (count($modes) > 1) {
            return [false, null];
        }

        return [true, $modes[0] ?? 'with'];
    }

    /** @param array<string, mixed> $where */
    private function isSoftDeleteWhere(QueryBuilder $query, array $where): bool
    {
        return in_array(
            strtolower((string) ($where['type'] ?? '')),
            ['null', 'notnull'],
            true,
        )
            && strtolower((string) ($where['boolean'] ?? 'and')) === 'and'
            && $this->isDeletedAtColumn($query, $where['column'] ?? null);
    }

    private function isValidQualifier(string $qualifier, string $table, ?string $alias): bool
    {
        return $alias !== null
            ? $qualifier === $alias
            : $qualifier === $table;
    }

    private function isDeletedAtColumn(QueryBuilder $query, mixed $column): bool
    {
        if (!is_string($column) || !is_string($query->deletedAtColumn())) {
            return false;
        }

        if (str_contains($column, '.')) {
            $column = substr($column, strrpos($column, '.') + 1);
        }

        return strtolower($column) === strtolower($query->deletedAtColumn());
    }
}
