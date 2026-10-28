<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use NormCache\Database\QueryBuilder;
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
        bool $queryScoped = false,
        string $operation = 'select',
    ): QueryPlan {
        if (
            $queryScoped
            || $query->dependencies() !== []
            || !empty($query->joins)
            || (!empty($query->unions) && count($dependencies) > 1)
            || $query->configuredCacheContext() !== null
        ) {
            return QueryPlan::result($root, $dependencies);
        }

        $wildcard = $this->isWildcard($query, $root);
        $primaryKey = $this->canUseRowShape($query, $operation)
            && $wildcard
                ? $query->primaryKey()
                : null;

        if ($primaryKey === null) {
            return QueryPlan::result($root, $dependencies);
        }

        return $this->directPlan($query, $root, $dependencies, $primaryKey)
            ?? QueryPlan::canonical($root, $dependencies, $primaryKey);
    }

    /** @return array{tokens: list<string>, softDeleteMode: ?string}|null */
    public function knownPrimaryKeys(QueryBuilder $query, QueryPlan $plan): ?array
    {
        if (
            !$plan->isCanonical()
            || $plan->primaryKey === null
            || $plan->primaryKey->family !== PrimaryKeyMetadata::INTEGER
            || count($plan->dependencies) !== 1
            || $plan->dependencies[0]->hash !== $plan->root->hash
            || $query->configuredCacheContext() !== null
            || $query->limit !== null
            || $query->offset !== null
            || count($query->orders ?? []) !== 1
        ) {
            return null;
        }

        $order = $query->orders[0];
        if (
            ($order['type'] ?? null) === 'Raw'
            || ($order['direction'] ?? null) === null
            || !in_array(strtolower((string) $order['direction']), ['asc', 'desc'], true)
            || !is_string($order['column'] ?? null)
            || !$this->resolvesToColumn($query, $plan->root, $order['column'], $plan->primaryKey->column)
        ) {
            return null;
        }

        $predicate = $this->primaryKeyPredicate($query, $plan->root, $plan->primaryKey);
        if ($predicate === null) {
            return null;
        }
        [$inWhere, $mode] = $predicate;
        if (!in_array($inWhere['type'] ?? null, ['In', 'InRaw'], true) || !is_array($inWhere['values'] ?? null)) {
            return null;
        }

        $values = [];
        foreach ($inWhere['values'] as $value) {
            if ($value instanceof Expression) {
                return null;
            }
            $token = $plan->primaryKey->token($value);
            $decoded = $token === null ? null : $plan->primaryKey->valueFromToken($token);
            if (!is_int($decoded)) {
                return null;
            }
            $values[$token] = $decoded;
        }
        if (strtolower((string) $order['direction']) === 'desc') {
            arsort($values, SORT_REGULAR);
        } else {
            asort($values, SORT_REGULAR);
        }

        return ['tokens' => array_keys($values), 'softDeleteMode' => $mode];
    }

    /**
     * @param  list<TableIdentity>  $dependencies
     */
    private function directPlan(
        QueryBuilder $query,
        TableIdentity $root,
        array $dependencies,
        PrimaryKeyMetadata $primaryKey,
    ): ?QueryPlan {
        if (
            $query->configuredTag() !== null
            || $query->configuredTtl() !== null
            || count($dependencies) !== 1
            || $query->offset !== null
            || ($query->limit !== null && $query->limit !== 1)
            || !empty($query->orders)
        ) {
            return null;
        }

        $predicate = $this->primaryKeyPredicate($query, $root, $primaryKey);
        if ($predicate === null) {
            return null;
        }
        [$where, $softDeleteMode] = $predicate;
        if (
            ($where['type'] ?? null) !== 'Basic'
            || !in_array($where['operator'] ?? null, ['=', '=='], true)
        ) {
            return null;
        }
        $directToken = $primaryKey->token($where['value'] ?? null);
        if ($directToken === null) {
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

    private function canUseRowShape(QueryBuilder $query, string $operation): bool
    {
        if (
            $operation !== 'select'
            || $query->distinct
            || $query->aggregate !== null
            || $query->groupLimit !== null
            || !empty($query->groups)
            || !empty($query->havings)
            || !empty($query->unions)
        ) {
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

    private function isWildcard(QueryBuilder $query, TableIdentity $root): bool
    {
        if ($query->columns === null || $query->columns === ['*']) {
            return true;
        }

        if (count($query->columns) !== 1 || !is_string($query->columns[0])) {
            return false;
        }

        $column = trim($query->columns[0]);

        $alias = $this->fromAlias($query);

        return $alias !== null
            ? $column === $alias . '.*'
            : $column === $root->table . '.*';
    }

    private function fromAlias(QueryBuilder $query): ?string
    {
        if (
            is_string($query->from)
            && preg_match('/\\s+(?:as\\s+)?([^\\s]+)$/i', trim($query->from), $matches) === 1
        ) {
            return $matches[1];
        }

        return null;
    }

    /** @return array{0: array<string, mixed>, 1: ?string}|null */
    private function primaryKeyPredicate(
        QueryBuilder $query,
        TableIdentity $root,
        PrimaryKeyMetadata $primaryKey,
    ): ?array {
        $primaryWhere = null;
        $deletedAt = $query->deletedAtColumn();
        $mode = $deletedAt === null ? null : 'with';

        foreach ($query->wheres as $where) {
            if (
                strtolower((string) ($where['boolean'] ?? 'and')) !== 'and'
                || !is_string($where['column'] ?? null)
            ) {
                return null;
            }
            if ($deletedAt !== null && $this->resolvesToColumn($query, $root, $where['column'], $deletedAt)) {
                if (!in_array($where['type'] ?? null, ['Null', 'NotNull'], true) || $mode !== 'with') {
                    return null;
                }
                $mode = ($where['type'] ?? null) === 'NotNull' ? 'only' : 'default';

                continue;
            }

            if (
                $primaryWhere !== null
                || !$this->resolvesToColumn($query, $root, $where['column'], $primaryKey->column)
            ) {
                return null;
            }
            $primaryWhere = $where;
        }

        if ($primaryWhere === null) {
            return null;
        }

        return [$primaryWhere, $mode];
    }

    private function resolvesToColumn(
        QueryBuilder $query,
        TableIdentity $root,
        string $column,
        string $expected,
    ): bool {
        $parts = explode('.', trim($column));
        $unqualified = trim((string) array_pop($parts), '`"[]');

        if ($unqualified !== $expected) {
            return false;
        }

        if ($parts === []) {
            return true;
        }

        $qualifier = trim((string) array_pop($parts), '`"[]');

        if ($parts !== []) {
            return false;
        }
        $alias = $this->fromAlias($query);

        return $alias !== null ? $qualifier === $alias : $qualifier === $root->table;
    }
}
