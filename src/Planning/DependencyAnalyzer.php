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
    /** @var array<string, ?TableIdentity> */
    private array $modelIdentities = [];

    public function __construct(
        private TableIdentityResolver $tables,
    ) {}

    public function clear(?string $connection = null): void
    {
        if ($connection === null) {
            $this->modelIdentities = [];

            return;
        }

        $this->modelIdentities = array_filter(
            $this->modelIdentities,
            static fn(?TableIdentity $id, string $key): bool => !str_starts_with($key, $connection . ':'),
            ARRAY_FILTER_USE_BOTH,
        );
    }

    public function analyze(
        Connection $connection,
        CachingQueryBuilder $query,
        TableIdentity $root,
    ): DependencyAnalysis {
        $resolved = [$root->hash => $root];
        $visited = [];
        $opaque = false;

        $this->walk($connection, $query, $resolved, $visited, $opaque);

        $explicit = $query->normCacheDeclaredTables() !== []
            || $query->normCacheDeclaredModels() !== [];

        foreach ($query->normCacheDeclaredTables() as $table) {
            $identity = $this->tables->resolve($connection, $table);

            if ($identity === null) {
                $opaque = true;
            } else {
                $resolved[$identity->hash] = $identity;
            }
        }

        foreach ($query->normCacheDeclaredModels() as $modelClass) {
            $identity = $this->modelIdentity($connection, $modelClass);

            if ($identity === null) {
                $opaque = true;
            } else {
                $resolved[$identity->hash] = $identity;
            }
        }

        ksort($resolved, SORT_STRING);

        return new DependencyAnalysis(array_values($resolved), $opaque, $explicit);
    }

    /** @param class-string $modelClass */
    public function modelIdentity(Connection $fallbackConnection, string $modelClass): ?TableIdentity
    {
        $cacheKey = $fallbackConnection->getName() . ':' . spl_object_id($fallbackConnection) . ':' . $modelClass;

        if (array_key_exists($cacheKey, $this->modelIdentities)) {
            return $this->modelIdentities[$cacheKey];
        }

        try {
            $model = new $modelClass;
            $connection = $model->getConnection();
            $identity = $this->tables->resolve($connection, $model->getTable());
        } catch (\Throwable) {
            $identity = null;
        }

        $this->modelIdentities[$cacheKey] = $identity;

        return $identity;
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
        $this->resolveSource($connection, $query->from, $resolved, $opaque);

        foreach ($query->joins ?? [] as $join) {
            $this->resolveSource($connection, $join->table ?? null, $resolved, $opaque);
            $this->walkNestedValues($connection, $join->wheres ?? [], $resolved, $visited, $opaque);
        }

        $this->walkNestedValues($connection, $query->wheres, $resolved, $visited, $opaque);
        $this->walkNestedValues($connection, $query->havings ?? [], $resolved, $visited, $opaque);
        $this->walkProjectionValues($query, $query->columns ?? [], $opaque);
        $this->walkOpaqueValues($query->groups ?? [], $opaque);
        $this->walkOpaqueValues($query->orders ?? [], $opaque);
        $this->walkOpaqueValues($query->unionOrders ?? [], $opaque);

        foreach ($query->unions ?? [] as $union) {
            $nested = $union['query'] ?? null;

            if ($nested instanceof Builder) {
                $this->walk($connection, $nested, $resolved, $visited, $opaque);
            } else {
                $opaque = true;
            }
        }
    }

    /** @param array<mixed> $values */
    private function walkOpaqueValues(array $values, bool &$opaque): void
    {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                $opaque = true;

                continue;
            }

            if (is_array($value)) {
                if (in_array(
                    strtolower((string) ($value['type'] ?? '')),
                    ['raw', 'expression'],
                    true,
                )) {
                    $opaque = true;
                }

                $this->walkOpaqueValues($value, $opaque);
            }
        }
    }

    /** @param array<mixed> $values */
    private function walkProjectionValues(
        Builder $query,
        array $values,
        bool &$opaque,
    ): void {
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                $sql = strtolower((string) $value->getValue($query->getGrammar()));

                if (preg_match('/\\b(?:select|from|join)\\b/', $sql) === 1) {
                    $opaque = true;
                }

                continue;
            }

            if (is_array($value)) {
                $this->walkProjectionValues($query, $value, $opaque);
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
                $this->walk($connection, $value, $resolved, $visited, $opaque);

                continue;
            }

            if ($value instanceof Expression) {
                $opaque = true;

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
