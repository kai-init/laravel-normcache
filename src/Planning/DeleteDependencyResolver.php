<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Support\SchemaCache;
use NormCache\Values\TableIdentity;

final readonly class DeleteDependencyResolver
{
    public function __construct(private TableIdentityResolver $tables, private SchemaCache $schema) {}

    private const RESTRICTING = 'restrict';

    /** @return list<TableIdentity>|null */
    public function affectedByDelete(Connection $connection, TableIdentity $parent, string $epoch): ?array
    {
        return $this->reachable($connection, $parent, $epoch, everyReference: false);
    }

    /** @return list<TableIdentity>|null */
    public function affectedByTruncate(Connection $connection, TableIdentity $parent, string $epoch): ?array
    {
        return $this->reachable($connection, $parent, $epoch, everyReference: true);
    }

    /** @return list<TableIdentity>|null */
    private function reachable(
        Connection $connection,
        TableIdentity $parent,
        string $epoch,
        bool $everyReference,
    ): ?array {
        $graph = $this->graph($connection, $epoch);

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
    private function graph(Connection $connection, string $epoch): ?array
    {
        try {
            return $this->schema->remember($connection, $epoch, 'deletes', fn(): array => $this->inspect($connection));
        } catch (\Throwable) {
            return null;
        }
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
