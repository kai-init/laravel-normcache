<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Exceptions\CascadeException;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\TableIdentity;

final class CascadeDependencyResolver
{
    public function __construct(
        private readonly CacheConfig $config,
        private readonly RedisStore $store,
        private readonly SchemaRepository $persistent,
        private readonly TableIdentityResolver $tables,
    ) {}

    /** @return list<TableIdentity>|null */
    public function affectedByDelete(Connection $connection, TableIdentity $parent): ?array
    {
        $graph = $this->persistent->deleteActions($connection);

        if ($graph === null) {
            return null;
        }

        $affected = [];
        $visited = [$parent->hash => true];
        $pending = [$parent];

        while (($current = array_pop($pending)) !== null) {
            foreach ($graph[$current->hash] ?? [] as $edge) {
                $child = $this->tables->resolveBaseTable($connection, $edge['table']);

                if ($child === null) {
                    throw $this->failure(
                        $connection,
                        'cached_child_identity',
                        childReference: $edge['table'],
                    );
                }

                $affected[$child->hash] = $child;

                if ($edge['action'] !== 'cascade' || isset($visited[$child->hash])) {
                    continue;
                }

                $visited[$child->hash] = true;
                $pending[] = $child;
            }
        }

        return array_values($affected);
    }

    public function warm(Connection $connection): void
    {
        $this->graph($connection);
    }

    /** @return array<string, list<array{table: string, action: string}>> */
    private function graph(Connection $connection): array
    {
        $graph = $this->persistent->deleteActions($connection);

        if ($graph !== null) {
            return $graph;
        }

        $lease = $this->claimBuild($connection);

        if ($lease === null) {
            return $this->inspectAndPersist($connection);
        }

        [$owner, $buildingKey, $token] = $lease;
        $graph = $this->persistent->deleteActions($connection, fresh: true);

        if (!$owner) {
            if ($graph !== null) {
                return $graph;
            }

            throw $this->failure($connection, 'build_lock_contended');
        }

        try {
            return $graph ?? $this->inspectAndPersist($connection);
        } finally {
            try {
                $this->store->releaseBuilding($buildingKey, '', $token);
            } catch (\Throwable) {
                // The lease expires independently; graph correctness is unaffected.
            }
        }
    }

    /** @return array<string, list<array{table: string, action: string}>> */
    private function inspectAndPersist(Connection $connection): array
    {
        $graph = $this->inspect($connection);
        $this->persistent->putDeleteActions($connection, $graph);

        return $graph;
    }

    /** @return array{bool, string, string}|null */
    private function claimBuild(Connection $connection): ?array
    {
        try {
            $buildingKey = $this->persistent->deleteActionsBuildKey($connection);
            $candidate = bin2hex(random_bytes(16));
            [$owner, $ownerToken] = $this->store->claimBuild(
                $buildingKey,
                $candidate,
                $this->config->buildingLockTtl,
            );

            if ($owner && $ownerToken === null) {
                return null;
            }

            return [$owner, $buildingKey, $ownerToken ?? ''];
        } catch (\Throwable) {
            return null;
        }
    }

    /** @return array<string, list<array{table: string, action: string}>> */
    private function inspect(Connection $connection): array
    {
        $prefix = (string) $connection->getTablePrefix();
        $graph = [];
        $schema = $connection->getSchemaBuilder();

        try {
            $tables = $schema->getTables();
        } catch (\Throwable $exception) {
            throw $this->failure($connection, 'table_listing', previous: $exception);
        }

        foreach ($tables as $table) {
            $childSchema = $table['schema'];
            $childTable = $this->logicalTable($table['name'], $prefix);

            if ($childTable === null) {
                continue;
            }

            $childReference = $this->reference($childSchema, $childTable);

            try {
                $foreignKeys = $schema->getForeignKeys($childReference);
            } catch (\Throwable $exception) {
                throw $this->failure(
                    $connection,
                    'foreign_key_listing',
                    childReference: $childReference,
                    previous: $exception,
                );
            }

            foreach ($foreignKeys as $foreignKey) {
                $parentTable = $this->logicalTable($foreignKey['foreign_table'], $prefix);

                if ($parentTable === null) {
                    continue;
                }

                $parentReference = $this->reference(
                    $foreignKey['foreign_schema'],
                    $parentTable,
                );
                $action = $this->deleteAction($foreignKey['on_delete']);

                if ($action === null) {
                    throw $this->failure(
                        $connection,
                        'delete_action',
                        childReference: $childReference,
                        parentReference: $parentReference,
                        unexpectedAction: $foreignKey['on_delete'],
                    );
                }

                if ($action === false) {
                    continue;
                }

                $child = $this->tables->resolveBaseTable($connection, $childReference);

                if ($child === null) {
                    throw $this->failure(
                        $connection,
                        'child_identity',
                        childReference: $childReference,
                        parentReference: $parentReference,
                    );
                }

                $parent = $this->tables->resolveBaseTable($connection, $parentReference);

                if ($parent === null) {
                    throw $this->failure(
                        $connection,
                        'parent_identity',
                        childReference: $childReference,
                        parentReference: $parentReference,
                    );
                }

                $graph[$parent->hash][$child->hash . "\0" . $action] = [
                    'table' => $child->qualifiedTable(),
                    'action' => $action,
                ];
            }
        }

        ksort($graph);

        foreach ($graph as $parent => $edges) {
            ksort($edges);
            $graph[$parent] = array_values($edges);
        }

        return $graph;
    }

    private function logicalTable(string $physical, string $prefix): ?string
    {
        if ($prefix === '') {
            return $physical !== '' ? $physical : null;
        }

        if (!str_starts_with($physical, $prefix)) {
            return null;
        }

        $logical = substr($physical, strlen($prefix));

        return $logical !== '' ? $logical : null;
    }

    private function reference(?string $schema, string $table): string
    {
        return $schema === null || $schema === '' ? $table : $schema . '.' . $table;
    }

    private function deleteAction(?string $action): string|false|null
    {
        if ($action === null) {
            return null;
        }

        return match ($action) {
            'cascade' => 'cascade',
            'set null' => 'set null',
            'set default' => 'set default',
            'no action', 'restrict' => false,
            default => null,
        };
    }

    private function failure(
        Connection $connection,
        string $stage,
        ?string $childReference = null,
        ?string $parentReference = null,
        ?string $unexpectedAction = null,
        ?\Throwable $previous = null,
    ): CascadeException {
        return new CascadeException(
            stage: $stage,
            driver: (string) $connection->getDriverName(),
            connection: (string) $connection->getName(),
            childReference: $childReference,
            parentReference: $parentReference,
            unexpectedAction: $unexpectedAction,
            previous: $previous,
        );
    }
}
