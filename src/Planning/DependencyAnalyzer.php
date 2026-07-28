<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use NormCache\Database\CachingQueryBuilder;
use NormCache\Values\DependencyAnalysis;
use NormCache\Values\TableIdentity;

final class DependencyAnalyzer
{
    public function __construct(
        private TableIdentityResolver $tables,
    ) {}

    public function analyze(
        Connection $connection,
        CachingQueryBuilder $query,
        TableIdentity $root,
    ): DependencyAnalysis {
        $resolved = [$root->hash => $root];
        $visited = [];
        $opaque = false;
        $volatile = false;

        $this->walk(
            $connection,
            $query,
            $resolved,
            $visited,
            $opaque,
            $volatile,
        );

        $declarations = $query->normCacheDependencies();
        $explicit = $declarations !== [];

        foreach ($declarations as $declaration) {
            $identity = $declaration->isTable()
                ? $this->tables->resolve($connection, $declaration->value)
                : $this->modelIdentity($connection, $declaration->value);

            if ($identity === null) {
                $opaque = true;
            } else {
                $resolved[$identity->hash] = $identity;
            }
        }

        ksort($resolved, SORT_STRING);

        return new DependencyAnalysis(
            array_values($resolved),
            $opaque,
            $explicit,
            $volatile,
        );
    }

    /** @param class-string $modelClass */
    public function modelIdentity(Connection $fallbackConnection, string $modelClass): ?TableIdentity
    {
        try {
            $model = new $modelClass;
            $model->setConnection($fallbackConnection->getName());

            return $this->tables->resolve($fallbackConnection, $model->getTable());
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
        bool &$volatile,
    ): void {
        $id = spl_object_id($query);

        if (isset($visited[$id])) {
            return;
        }

        $visited[$id] = true;
        $this->walkProjectionValues($query, [$query->from], $opaque, $volatile);
        $this->resolveSource($connection, $query->from, $resolved, $opaque);

        foreach ($query->joins ?? [] as $join) {
            $this->walkProjectionValues(
                $query,
                [$join->table ?? null],
                $opaque,
                $volatile,
            );
            $this->resolveSource($connection, $join->table ?? null, $resolved, $opaque);
            $this->walkNestedValues(
                $connection,
                $join->wheres ?? [],
                $resolved,
                $visited,
                $opaque,
                $volatile,
            );
        }

        $this->walkNestedValues(
            $connection,
            $query->wheres,
            $resolved,
            $visited,
            $opaque,
            $volatile,
        );
        $this->walkNestedValues(
            $connection,
            $query->havings ?? [],
            $resolved,
            $visited,
            $opaque,
            $volatile,
        );
        $this->walkProjectionValues($query, $query->columns ?? [], $opaque, $volatile);
        $this->walkOpaqueValues($query, $query->groups ?? [], $opaque, $volatile);
        $this->walkOpaqueValues($query, $query->orders ?? [], $opaque, $volatile);
        $this->walkOpaqueValues($query, $query->unionOrders ?? [], $opaque, $volatile);

        foreach ($query->unions ?? [] as $union) {
            $nested = $union['query'] ?? null;

            if ($nested instanceof Builder) {
                $this->walk(
                    $connection,
                    $nested,
                    $resolved,
                    $visited,
                    $opaque,
                    $volatile,
                );
            } else {
                $opaque = true;
            }
        }
    }

    /** @param array<mixed> $values */
    private function walkOpaqueValues(
        Builder $query,
        array $values,
        bool &$opaque,
        bool &$volatile,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                $opaque = true;
                $volatile = $volatile || $this->isVolatileSql(
                    (string) $value->getValue($query->getGrammar()),
                );

                continue;
            }

            if (is_array($value)) {
                if (in_array(
                    strtolower((string) ($value['type'] ?? '')),
                    ['raw', 'expression'],
                    true,
                )) {
                    $opaque = true;
                    $volatile = $volatile || $this->hasVolatileSqlValue($value);
                }

                $this->walkOpaqueValues($query, $value, $opaque, $volatile);
            }
        }
    }

    /** @param array<mixed> $values */
    private function walkProjectionValues(
        Builder $query,
        array $values,
        bool &$opaque,
        bool &$volatile,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                $sql = (string) $value->getValue($query->getGrammar());
                $volatile = $volatile || $this->isVolatileSql($sql);

                if (preg_match('/\b(?:select|from|join)\b/i', $sql) === 1) {
                    $opaque = true;
                }

                continue;
            }

            if (is_array($value)) {
                $this->walkProjectionValues($query, $value, $opaque, $volatile);
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
        bool &$volatile,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Builder) {
                $this->walk(
                    $connection,
                    $value,
                    $resolved,
                    $visited,
                    $opaque,
                    $volatile,
                );

                continue;
            }

            if ($value instanceof Expression) {
                $opaque = true;
                $volatile = $volatile || $this->isVolatileSql(
                    (string) $value->getValue($connection->getQueryGrammar()),
                );

                continue;
            }

            if (!is_array($value)) {
                continue;
            }

            if (in_array(
                strtolower((string) ($value['type'] ?? '')),
                ['raw', 'expression'],
                true,
            )) {
                $opaque = true;
                $volatile = $volatile || $this->hasVolatileSqlValue($value);
            }

            $this->walkNestedValues(
                $connection,
                array_values($value),
                $resolved,
                $visited,
                $opaque,
                $volatile,
            );
        }
    }

    /** @param array<mixed> $values */
    private function hasVolatileSqlValue(array $values): bool
    {
        foreach (['sql', 'value'] as $key) {
            if (is_string($values[$key] ?? null) && $this->isVolatileSql($values[$key])) {
                return true;
            }
        }

        return false;
    }

    private function isVolatileSql(string $sql): bool
    {
        return preg_match(
            '/\b(?:rand|random|randomblob|uuid|uuid_short|newid|newsequentialid|gen_random_uuid|uuid_generate_v[0-9]+|nextval|currval|lastval|last_insert_id|last_insert_rowid|changes|total_changes|row_count|found_rows|connection_id|pg_backend_pid|now|sysdate|getdate|sysdatetime|sysutcdatetime|utc_timestamp|utc_date|utc_time|curdate|curtime|clock_timestamp|statement_timestamp|transaction_timestamp|timeofday|sleep|pg_sleep|benchmark)\s*\(/i',
            $sql,
        ) === 1
            || preg_match(
                '/\b(?:current_timestamp|current_date|current_time|localtimestamp|localtime|current_user|session_user|system_user)\b/i',
                $sql,
            ) === 1
            || preg_match(
                '/\b(?:date|time|datetime|julianday|unixepoch|strftime)\s*\([^)]*[\'\"]now[\'\"]/i',
                $sql,
            ) === 1;
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
