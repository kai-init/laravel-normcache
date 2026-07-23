<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\ProjectionClassifier;
use NormCache\Values\DependencySet;

final class QueryAnalyzer
{
    private const DIRECT_PRIMARY_KEY_BLOCKERS = QueryInspection::NON_CANONICAL_FROM
        | QueryInspection::JOIN
        | QueryInspection::GROUP
        | QueryInspection::HAVING
        | QueryInspection::UNION
        | QueryInspection::AGGREGATE
        | QueryInspection::DISTINCT
        | QueryInspection::LOCK
        | QueryInspection::CALCULATED_COLUMNS;

    public function __construct(
        private readonly CacheKeyBuilder $keys = new CacheKeyBuilder,
    ) {}

    public const EXISTS_WHERE_TYPES = [
        'Exists' => true,
        'NotExists' => true,
    ];

    public const SUBQUERY_WHERE_TYPES = [
        'Sub' => true,
        'InSub' => true,
        'NotInSub' => true,
    ];

    public function inspect(
        QueryBuilder $base,
        string $table,
        ?array $resolvedColumns,
        array $primaryKeyIdentifiers = [],
        ?string $softDeleteScopeColumn = null,
        string|\Closure|null $connection = null,
        ?DependencySet $capturedDependencies = null,
        array $contextReasons = [],
        int $capturedOpaqueJoins = 0,
        bool $capturedOpaqueFrom = false,
        int $capturedOpaqueWhereSubqueries = 0,
        bool $allowPrimaryKeyFastPath = false,
    ): QueryInspection {
        $primaryKeys = $primaryKeyIdentifiers === []
            ? null
            : self::resolvePrimaryKeys($base, $primaryKeyIdentifiers, $softDeleteScopeColumn);
        $capturedDependencies ??= DependencySet::empty();
        $structuralFlags = $this->structuralFlags($base, $table, $resolvedColumns);
        $rawOrderFlags = $this->rawOrderFlags($base);

        if ($allowPrimaryKeyFastPath
            && $contextReasons === []
            && $capturedDependencies->safe
            && $capturedDependencies->hasNoDependencies()
            && $capturedOpaqueJoins === 0
            && !$capturedOpaqueFrom
            && $capturedOpaqueWhereSubqueries === 0
            && $primaryKeys !== null
            && ($structuralFlags & self::DIRECT_PRIMARY_KEY_BLOCKERS) === 0) {
            return new QueryInspection($rawOrderFlags, $primaryKeys, $capturedDependencies);
        }

        $dependencies = $connection === null
            ? $capturedDependencies
            : $this->inferQueryDependencies(
                $base,
                $connection instanceof \Closure ? $connection() : $connection,
                $table,
                $capturedOpaqueJoins,
                $capturedOpaqueFrom,
                $capturedOpaqueWhereSubqueries,
            )->merge($capturedDependencies);

        return new QueryInspection(
            flags: $structuralFlags | $rawOrderFlags | $this->inspectWheres((array) $base->wheres),
            primaryKeys: $primaryKeys,
            dependencies: $dependencies,
            contextReasons: $contextReasons,
        );
    }

    public function flags(
        QueryBuilder $base,
        string $table,
        ?array $resolvedColumns,
    ): int {
        $flags = $this->structuralFlags($base, $table, $resolvedColumns);

        $flags |= $this->rawOrderFlags($base);
        $flags |= $this->inspectWheres((array) $base->wheres);

        return $flags;
    }

    private function rawOrderFlags(QueryBuilder $base): int
    {
        foreach ((array) $base->orders as $order) {
            if (($order['type'] ?? null) === 'Raw') {
                return QueryInspection::RAW_ORDER;
            }
        }

        return 0;
    }

    private function structuralFlags(QueryBuilder $base, string $table, ?array $resolvedColumns): int
    {
        $flags = 0;

        if ($base->from !== $table) {
            $flags |= QueryInspection::NON_CANONICAL_FROM;
        }

        if (!empty($base->joins)) {
            $flags |= QueryInspection::JOIN;
        }

        if (!empty($base->groups)) {
            $flags |= QueryInspection::GROUP;
        }

        if (!empty($base->havings)) {
            $flags |= QueryInspection::HAVING;
        }

        if (!empty($base->unions)) {
            $flags |= QueryInspection::UNION;
        }

        if (!empty($base->aggregate)) {
            $flags |= QueryInspection::AGGREGATE;
        }

        if (!empty($base->distinct)) {
            $flags |= QueryInspection::DISTINCT;
        }

        if ($base->lock !== null) {
            $flags |= QueryInspection::LOCK;
        }

        if (ProjectionClassifier::hasCalculatedColumns($resolvedColumns)) {
            $flags |= QueryInspection::CALCULATED_COLUMNS;
        }

        return $flags;
    }

    public function extractTables(QueryBuilder $base, string $table): array
    {
        if (empty($base->joins)) {
            return [$table];
        }

        $tables = [$table];

        foreach ((array) $base->joins as $join) {
            if (is_string($join->table)) {
                $tables[] = CacheKeyBuilder::stripTableAlias($join->table);
            }
        }

        return array_values(array_unique($tables));
    }

    /** @param string $connection  Eloquent connection name, e.g. $model->getConnection()->getName() */
    public function inferQueryDependencies(
        QueryBuilder $base,
        string $connection,
        ?string $primaryTable = null,
        int $capturedOpaqueJoins = 0,
        bool $capturedOpaqueFrom = false,
        int $capturedOpaqueWhereSubqueries = 0,
    ): DependencySet {
        $connection = $this->connectionName($base, $connection);
        $tables = [];
        $unsafe = $this->collectQueryTables(
            $base,
            $connection,
            $tables,
            $capturedOpaqueJoins,
            $capturedOpaqueFrom,
        );

        if ($unsafe !== null) {
            return DependencySet::unsafe($unsafe);
        }

        if ($this->countOpaqueWhereSubqueries($base) > $capturedOpaqueWhereSubqueries) {
            return DependencySet::unsafe('subquery predicate dependency could not be inferred');
        }

        if ($primaryTable !== null) {
            unset($tables[$this->keys->tableKey($connection, $primaryTable)]);
        }

        return new DependencySet(tables: array_keys($tables));
    }

    /** @param array<string, true> $tables */
    private function collectQueryTables(
        QueryBuilder $base,
        string $connection,
        array &$tables,
        int $capturedOpaqueJoins = 0,
        bool $capturedOpaqueFrom = false,
    ): ?string {
        $connection = $this->connectionName($base, $connection);

        if (is_string($base->from)) {
            if ($this->joinTableHasImplicitAlias($base->from)) {
                return 'query source dependency could not be inferred';
            }

            $tables[$this->keys->tableKey($connection, $base->from)] = true;
        } elseif (!$capturedOpaqueFrom) {
            return 'query source dependency could not be inferred';
        }

        $opaqueJoins = 0;
        foreach ($base->joins ?? [] as $join) {
            if ($this->joinClauseHasComplexWheres($join->wheres ?? [])) {
                return 'join clause dependency could not be inferred';
            }

            if (is_string($join->table)) {
                if ($this->joinTableHasImplicitAlias($join->table)) {
                    return 'join table alias could not be inferred';
                }

                $tables[$this->keys->tableKey($connection, $join->table)] = true;
            } else {
                $opaqueJoins++;
            }
        }

        if ($opaqueJoins > $capturedOpaqueJoins) {
            return 'joined subquery dependency could not be inferred';
        }

        foreach ($base->wheres as $where) {
            $query = $where['query'] ?? null;

            if ($query instanceof QueryBuilder
                && ($reason = $this->collectQueryTables($query, $connection, $tables))) {
                return $reason;
            }
        }

        foreach ($base->unions ?? [] as $union) {
            $query = $union['query'] ?? null;

            if (!$query instanceof QueryBuilder) {
                return 'union dependency could not be inferred';
            }

            if ($reason = $this->collectQueryTables($query, $connection, $tables)) {
                return $reason;
            }
        }

        return null;
    }

    private function countOpaqueWhereSubqueries(QueryBuilder $base): int
    {
        $count = 0;

        foreach ($base->wheres as $where) {
            $type = $where['type'] ?? null;

            if ($type === 'Basic' && ($where['column'] ?? null) instanceof Expression) {
                $count++;
            }

            if (($type === 'In' || $type === 'NotIn') && isset($where['values'])) {
                foreach ((array) $where['values'] as $value) {
                    if ($value instanceof Expression) {
                        $count++;
                    }
                }
            }

            if (($where['query'] ?? null) instanceof QueryBuilder) {
                $count += $this->countOpaqueWhereSubqueries($where['query']);
            }
        }

        return $count;
    }

    private function connectionName(QueryBuilder $base, string $fallback): string
    {
        /** @var Connection $connection */
        $connection = $base->getConnection();

        return $connection->getName() ?? $fallback;
    }

    /** @deprecated Use inferQueryDependencies(). */
    public function inferJoinDependencies(QueryBuilder $base, string $connection): DependencySet
    {
        $dependencies = $this->inferQueryDependencies($base, $connection, is_string($base->from) ? $base->from : null);

        return new DependencySet(
            tables: $dependencies->tables,
            safe: $dependencies->safe,
            reasons: $dependencies->reasons,
        );
    }

    private function joinTableHasImplicitAlias(string $table): bool
    {
        return (bool) preg_match('/\s+/', trim($table)) && !preg_match('/\s+as\s+/i', $table);
    }

    private function joinClauseHasComplexWheres(array $wheres): bool
    {
        foreach ($wheres as $where) {
            if (!in_array($where['type'] ?? null, ['Column', 'Basic', 'Null', 'NotNull'], true)) {
                return true;
            }
        }

        return false;
    }

    public static function resolvePrimaryKeys(
        QueryBuilder $base,
        array $primaryKeyIdentifiers,
        ?string $softDeleteScopeColumn = null,
    ): ?array {
        if ($base->offset > 0) {
            return null;
        }

        if ($base->limit === 0) {
            return [];
        }

        $wheres = array_values(array_filter(
            $base->wheres,
            static fn(array $where): bool => !self::isSoftDeleteScopeConstraint($where, $softDeleteScopeColumn),
        ));

        if (count($wheres) !== 1) {
            return null;
        }

        $where = $wheres[0];
        $column = $where['column'] ?? null;

        if (!in_array($column, $primaryKeyIdentifiers, true)) {
            return null;
        }

        if (($where['type'] ?? null) === 'Basic' && ($where['operator'] ?? null) === '=') {
            return $where['value'] instanceof Expression ? null : [$where['value']];
        }

        if (!empty($base->orders) || $base->limit > 0) {
            return null;
        }

        if (($where['type'] ?? null) === 'In' || (($where['type'] ?? null) === 'InRaw' && isset($where['values']))) {
            $values = (array) $where['values'];

            foreach ($values as $value) {
                if ($value instanceof Expression) {
                    return null;
                }
            }

            sort($values);

            return $values;
        }

        return null;
    }

    private static function isSoftDeleteScopeConstraint(array $where, ?string $column): bool
    {
        return $column !== null
            && ($where['type'] ?? null) === 'Null'
            && ($where['boolean'] ?? 'and') === 'and'
            && !($where['not'] ?? false)
            && ($where['column'] ?? null) === $column;
    }

    private function inspectWheres(array $wheres): int
    {
        $flags = 0;

        foreach ($wheres as $where) {
            $type = $where['type'] ?? '';

            if ($type === 'raw') {
                $flags |= QueryInspection::RAW_WHERE;
            }

            $flags |= match (true) {
                isset(self::EXISTS_WHERE_TYPES[$type]) => QueryInspection::EXISTS_WHERE,
                isset(self::SUBQUERY_WHERE_TYPES[$type]) => QueryInspection::SUBQUERY_WHERE,
                ($type === 'In' || $type === 'NotIn') && $this->containsExpression((array) ($where['values'] ?? [])) => QueryInspection::SUBQUERY_WHERE,
                $type === 'Basic' && ($where['column'] ?? null) instanceof Expression => QueryInspection::SUBQUERY_WHERE,
                default => 0,
            };

            if (($where['query'] ?? null) instanceof QueryBuilder) {
                if ($where['query']->lock !== null) {
                    $flags |= QueryInspection::LOCK;
                }

                $flags |= $this->inspectWheres((array) $where['query']->wheres);
            }

            if (($flags & (QueryInspection::RAW_WHERE | QueryInspection::SUBQUERY_WHERE))
                === (QueryInspection::RAW_WHERE | QueryInspection::SUBQUERY_WHERE)) {
                break;
            }
        }

        return $flags;
    }

    private function containsExpression(array $values): bool
    {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                return true;
            }
        }

        return false;
    }
}
