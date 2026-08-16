<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use NormCache\Database\QueryBuilder;
use NormCache\Support\ColumnName;
use NormCache\Values\DependencyAnalysis;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlanner
{
    public function __construct(
        private readonly PredicateColumnExtractor $predicateColumns = new PredicateColumnExtractor,
    ) {}

    /**
     * @param  list<TableIdentity>  $dependencies
     */
    public function plan(
        QueryBuilder $query,
        TableIdentity $root,
        array $dependencies,
        bool $forceQueryGroup = false,
        string $operation = 'select',
    ): QueryPlan {
        if (
            $forceQueryGroup
            || $query->joins !== null && $query->joins !== []
            || $this->hasCrossTableUnion($root, $dependencies, $query)
        ) {
            return QueryPlan::queryGroup($root, $dependencies);
        }

        if ($query->configuredCacheContext() !== null) {
            return QueryPlan::result($root, $dependencies);
        }

        $wildcard = $this->isWildcard($query, $root);
        $plainColumns = $wildcard ? null : $this->plainColumns($query, $root);
        $primaryKey = $this->canUseRowShape($query, $operation)
            && ($wildcard || $plainColumns !== null)
                ? $query->primaryKey()
                : null;

        $direct = $this->directPlan(
            $query,
            $root,
            $dependencies,
            $primaryKey,
            $wildcard,
            $plainColumns,
        );

        if ($direct !== null) {
            return $direct;
        }

        if (
            $primaryKey !== null
            && $wildcard
        ) {
            return QueryPlan::canonical(
                $root,
                $dependencies,
                $primaryKey,
                $this->predicateColumns->extract($query),
            );
        }

        if ($primaryKey !== null && $plainColumns !== null) {
            return QueryPlan::projectedResult(
                $root,
                $dependencies,
                $primaryKey,
                $plainColumns,
                $this->predicateColumns->extract($query),
            );
        }

        return QueryPlan::result($root, $dependencies, $primaryKey);
    }

    /**
     * @param  list<TableIdentity>  $dependencies
     * @param  list<string>|null  $plainColumns
     */
    private function directPlan(
        QueryBuilder $query,
        TableIdentity $root,
        array $dependencies,
        ?PrimaryKeyMetadata $primaryKey,
        bool $wildcard,
        ?array $plainColumns,
    ): ?QueryPlan {
        if (
            $primaryKey === null
            || count($dependencies) !== 1
            || !$this->allowsDirectControls($query)
            || (!$wildcard && $plainColumns === null)
        ) {
            return null;
        }

        $directToken = $this->directPrimaryKeyToken($query, $root, $primaryKey);

        if ($directToken === null) {
            return null;
        }

        [$softDeleteSafe, $softDeleteMode] = $this->softDeleteMode($query, $root);

        if (!$softDeleteSafe) {
            return null;
        }

        $deletedAtColumn = $query->deletedAtColumn();

        return $wildcard
            ? QueryPlan::directPrimaryKey(
                $root,
                $dependencies,
                $primaryKey,
                $directToken,
                $softDeleteMode,
                $deletedAtColumn,
            )
            : QueryPlan::projectedRow(
                $root,
                $dependencies,
                $primaryKey,
                $directToken,
                (array) $plainColumns,
                $softDeleteMode,
                $deletedAtColumn,
            );
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

    private function canUseRowShape(QueryBuilder $query, string $operation): bool
    {
        return $operation === 'select' && $this->isSingleRowShape($query);
    }

    private function hasCrossTableUnion(
        TableIdentity $root,
        array $dependencies,
        QueryBuilder $query,
    ): bool {
        return !empty($query->unions)
            && DependencyAnalysis::hasExternalTo($root, $dependencies);
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
            if ($this->isSoftDeleteWhere($query, $root, $where)) {
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

        if (!$this->resolvesToColumn($query, $root, (string) $where['column'], $primaryKey->column)) {
            return null;
        }

        return $primaryKey->token($where['value'] ?? null);
    }

    /** @return array{0: bool, 1: ?string} */
    private function softDeleteMode(QueryBuilder $query, TableIdentity $root): array
    {
        if ($query->deletedAtColumn() === null) {
            return [true, null];
        }

        $modes = [];

        foreach ($query->wheres as $where) {
            if (!$this->isDeletedAtColumn($query, $root, $where['column'] ?? null)) {
                continue;
            }

            $type = $where['type'] ?? null;

            if (
                !in_array($type, ['Null', 'NotNull'], true)
                || strtolower((string) ($where['boolean'] ?? 'and')) !== 'and'
            ) {
                return [false, null];
            }

            $modes[] = $type === 'NotNull' ? 'only' : 'default';
        }

        if (count($modes) > 1) {
            return [false, null];
        }

        return [true, $modes[0] ?? 'with'];
    }

    /** @param array<string, mixed> $where */
    private function isSoftDeleteWhere(QueryBuilder $query, TableIdentity $root, array $where): bool
    {
        return in_array($where['type'] ?? null, ['Null', 'NotNull'], true)
            && strtolower((string) ($where['boolean'] ?? 'and')) === 'and'
            && $this->isDeletedAtColumn($query, $root, $where['column'] ?? null);
    }

    private function isValidQualifier(string $qualifier, string $table, ?string $alias): bool
    {
        return $alias !== null
            ? $qualifier === $alias
            : $qualifier === $table;
    }

    private function isDeletedAtColumn(QueryBuilder $query, TableIdentity $root, mixed $column): bool
    {
        $deletedAt = $query->deletedAtColumn();

        return is_string($column)
            && is_string($deletedAt)
            && $this->resolvesToColumn($query, $root, $column, $deletedAt);
    }

    private function resolvesToColumn(
        QueryBuilder $query,
        TableIdentity $root,
        string $column,
        string $expected,
    ): bool {
        $segments = ColumnName::segments($column);
        $unqualified = array_pop($segments);

        if ($unqualified !== strtolower($expected)) {
            return false;
        }

        if ($segments === []) {
            return true;
        }

        $qualifier = (string) array_pop($segments);

        return $segments === [] && $this->isValidQualifier(
            $qualifier,
            strtolower($root->table),
            $this->fromAlias($query),
        );
    }
}
