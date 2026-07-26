<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use NormCache\Database\CachingQueryBuilder;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\QueryPlan;
use NormCache\Values\TableIdentity;

final class QueryPlanner
{
    /** @param list<TableIdentity> $dependencies */
    public function plan(
        CachingQueryBuilder $query,
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

        if (
            $operation === 'select'
            && $primaryKey !== null
            && count($dependencies) === 1
            && $this->isCanonicalRowShape($query, $root)
            && $this->allowsDirectControls($query)
        ) {
            $directToken = $this->directPrimaryKeyToken($query, $primaryKey);

            if ($directToken !== null) {
                return new QueryPlan(
                    QueryPlan::DIRECT_PK,
                    $root,
                    $dependencies,
                    $primaryKey,
                    $directToken,
                    softDeleteMode: $this->softDeleteMode($query),
                    deletedAtColumn: $query->normCacheDeletedAtColumn(),
                );
            }
        }

        if (
            $operation === 'select'
            && $primaryKey !== null
            && $this->isCanonicalRowShape($query, $root)
        ) {
            return new QueryPlan(QueryPlan::CANONICAL, $root, $dependencies, $primaryKey);
        }

        return new QueryPlan(QueryPlan::EXACT, $root, $dependencies, $primaryKey);
    }

    private function allowsDirectControls(CachingQueryBuilder $query): bool
    {
        return $query->normCacheTag() === null && $query->normCacheTtl() === null;
    }

    private function isCanonicalRowShape(CachingQueryBuilder $query, TableIdentity $root): bool
    {
        return $this->isWildcard($query, $root)
            && !$query->distinct
            && $query->aggregate === null
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
        CachingQueryBuilder $query,
    ): bool {
        return !empty($query->unions)
            && count(array_filter(
                $dependencies,
                static fn(TableIdentity $dependency): bool => $dependency->hash !== $root->hash,
            )) > 0;
    }

    private function isWildcard(CachingQueryBuilder $query, TableIdentity $root): bool
    {
        if ($query->columns === null || $query->columns === ['*']) {
            return true;
        }

        if (count($query->columns) !== 1 || !is_string($query->columns[0])) {
            return false;
        }

        $column = strtolower(trim($query->columns[0]));

        if ($column === strtolower($root->table) . '.*') {
            return true;
        }

        if (!is_string($query->from)) {
            return false;
        }

        if (preg_match('/\\s+as\\s+([^\\s]+)$/i', trim($query->from), $matches) === 1) {
            return $column === strtolower($matches[1]) . '.*';
        }

        return false;
    }

    private function directPrimaryKeyToken(
        CachingQueryBuilder $query,
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

        $column = (string) $where['column'];
        $unqualified = $column;

        if (str_contains($column, '.')) {
            $unqualified = substr($column, strrpos($column, '.') + 1);
        }

        if (
            strtolower(trim($unqualified, '`"[]'))
            !== strtolower($primaryKey->column)
        ) {
            return null;
        }

        return $primaryKey->token($where['value'] ?? null);
    }

    private function softDeleteMode(CachingQueryBuilder $query): ?string
    {
        if ($query->normCacheDeletedAtColumn() === null) {
            return null;
        }

        foreach ($query->wheres as $where) {
            if (!$this->isDeletedAtColumn($query, $where['column'] ?? null)) {
                continue;
            }

            return strtolower((string) ($where['type'] ?? '')) === 'notnull'
                ? 'only'
                : 'default';
        }

        return 'with';
    }

    /** @param array<string, mixed> $where */
    private function isSoftDeleteWhere(CachingQueryBuilder $query, array $where): bool
    {
        return in_array(
            strtolower((string) ($where['type'] ?? '')),
            ['null', 'notnull'],
            true,
        )
            && strtolower((string) ($where['boolean'] ?? 'and')) === 'and'
            && $this->isDeletedAtColumn($query, $where['column'] ?? null);
    }

    private function isDeletedAtColumn(CachingQueryBuilder $query, mixed $column): bool
    {
        if (!is_string($column) || !is_string($query->normCacheDeletedAtColumn())) {
            return false;
        }

        if (str_contains($column, '.')) {
            $column = substr($column, strrpos($column, '.') + 1);
        }

        return strtolower($column) === strtolower($query->normCacheDeletedAtColumn());
    }
}
