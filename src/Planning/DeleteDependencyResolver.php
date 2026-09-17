<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Values\TableIdentity;

final class DeleteDependencyResolver
{
    /**
     * @var \WeakMap<Connection, array{
     *     signature: string,
     *     graph: array<string, list<array{table: string, action: string}>>
     * }>
     */
    private \WeakMap $connections;

    public function __construct(private readonly TableIdentityResolver $tables)
    {
        $this->connections = new \WeakMap;
    }

    public function clear(): void
    {
        $this->connections = new \WeakMap;
    }

    private const RESTRICTING = 'restrict';

    /** @return list<TableIdentity>|null */
    public function affectedByDelete(Connection $connection, TableIdentity $parent): ?array
    {
        return $this->reachable($connection, $parent, everyReference: false);
    }

    /** @return list<TableIdentity>|null */
    public function affectedByTruncate(Connection $connection, TableIdentity $parent): ?array
    {
        return $this->reachable($connection, $parent, everyReference: true);
    }

    /** @return list<TableIdentity>|null */
    private function reachable(
        Connection $connection,
        TableIdentity $parent,
        bool $everyReference,
    ): ?array {
        $graph = $this->graph($connection);

        if ($graph === null) {
            return null;
        }

        $affected = [];
        $visited = [$parent->hash => true];
        $pending = [$parent];

        while (($current = array_pop($pending)) !== null) {
            foreach ($graph[$current->hash] ?? [] as $edge) {
                if (!$everyReference && $edge['action'] === self::RESTRICTING) {
                    continue;
                }

                $child = $this->tables->resolve($connection, $edge['table']);

                if ($child === null) {
                    return null;
                }

                $affected[$child->hash] = $child;
                $descends = $everyReference || $edge['action'] === 'cascade';

                if (!$descends || isset($visited[$child->hash])) {
                    continue;
                }

                $visited[$child->hash] = true;
                $pending[] = $child;
            }
        }

        return array_values($affected);
    }

    /** @return array<string, list<array{table: string, action: string}>>|null */
    private function graph(Connection $connection): ?array
    {
        $sourceScope = ConnectionSourceResolver::resolve($connection);

        if ($sourceScope === null) {
            return null;
        }

        $signature = TableIdentity::encodeFields([
            $sourceScope,
            (string) $connection->getDriverName(),
            (string) $connection->getDatabaseName(),
            (string) $connection->getTablePrefix(),
        ]);
        $metadata = $this->connections[$connection] ?? null;

        if ($metadata !== null && $metadata['signature'] === $signature) {
            return $metadata['graph'];
        }

        try {
            $graph = $this->inspect($connection);
        } catch (\Throwable) {
            return null;
        }

        $this->connections[$connection] = [
            'signature' => $signature,
            'graph' => $graph,
        ];

        return $graph;
    }

    /** @return array<string, list<array{table: string, action: string}>> */
    private function inspect(Connection $connection): array
    {
        $prefix = (string) $connection->getTablePrefix();
        $schema = $connection->getSchemaBuilder();
        $graph = [];

        foreach ($schema->getTables() as $table) {
            $childTable = $this->logicalTable($table['name'], $prefix);

            if ($childTable === null) {
                continue;
            }

            $childReference = $this->reference($table['schema'], $childTable);

            foreach ($schema->getForeignKeys($childReference) as $foreignKey) {
                $parentTable = $this->logicalTable($foreignKey['foreign_table'], $prefix);

                if ($parentTable === null) {
                    continue;
                }

                $parentReference = $this->reference($foreignKey['foreign_schema'], $parentTable);
                $action = $this->deleteAction($foreignKey['on_delete']);

                if ($action === null) {
                    throw new \UnexpectedValueException('Foreign-key metadata contained an unknown delete action.');
                }

                $child = $this->tables->resolve($connection, $childReference);
                $parent = $this->tables->resolve($connection, $parentReference);

                if ($child === null || $parent === null) {
                    throw new \UnexpectedValueException('Foreign-key table identity could not be resolved.');
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

    private function deleteAction(?string $action): ?string
    {
        if ($action === null) {
            return null;
        }

        return match (strtolower(trim($action))) {
            'cascade' => 'cascade',
            'set null' => 'set null',
            'set default' => 'set default',
            'no action', 'restrict' => self::RESTRICTING,
            default => null,
        };
    }
}
