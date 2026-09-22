<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use NormCache\Database\QueryBuilder;
use NormCache\Values\DependencyAnalysis;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlanner
{
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
            || $query->dependencies() !== []
            || $query->joins !== null && $query->joins !== []
            || $this->hasCrossTableUnion($root, $dependencies, $query)
        ) {
            return QueryPlan::queryGroup($root, $dependencies);
        }

        if ($query->configuredCacheContext() !== null) {
            return QueryPlan::result($root, $dependencies);
        }

        $wildcard = $this->isWildcard($query, $root);
        $primaryKey = $this->canUseRowShape($query, $operation)
            && $wildcard
                ? $query->primaryKey()
                : null;

        $direct = $this->directPlan(
            $query,
            $root,
            $dependencies,
            $primaryKey,
        );

        if ($direct !== null) {
            return $direct;
        }

        if ($primaryKey !== null) {
            return QueryPlan::canonical(
                $root,
                $dependencies,
                $primaryKey,
            );
        }

        return QueryPlan::result($root, $dependencies);
    }

    /**
     * @param  list<TableIdentity>  $dependencies
     */
    private function directPlan(
        QueryBuilder $query,
        TableIdentity $root,
        array $dependencies,
        ?PrimaryKeyMetadata $primaryKey,
    ): ?QueryPlan {
        if (
            $primaryKey === null
            || count($dependencies) !== 1
            || !$this->allowsDirectControls($query)
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

        return QueryPlan::directPrimaryKey(
            $root,
            $dependencies,
            $primaryKey,
            $directToken,
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
        if ($operation !== 'select' || !$this->isSingleRowShape($query)) {
            return false;
        }

        // Replacing the wildcard can change positional ordering in raw expressions.
        foreach ($query->orders ?? [] as $order) {
            if (($order['type'] ?? null) === 'Raw' || $order['column'] instanceof Expression) {
                return false;
            }
        }

        return true;
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
        $parts = explode('.', trim($column));
        $unqualified = trim((string) array_pop($parts), '`"[]');

        if (strtolower($unqualified) !== strtolower($expected)) {
            return false;
        }

        if ($parts === []) {
            return true;
        }

        $qualifier = strtolower(trim((string) array_pop($parts), '`"[]'));

        return $parts === [] && $this->isValidQualifier(
            $qualifier,
            strtolower($root->table),
            $this->fromAlias($query),
        );
    }
}
