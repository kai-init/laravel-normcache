<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Database\QueryBuilder;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

final class PrimaryKeyResolver
{
    /** @var array<string, array{connection: string, metadata: PrimaryKeyMetadata|null}> */
    private array $memo = [];

    /** @var array<string, true> */
    private array $warnedConflicts = [];

    public function __construct(
        private readonly CacheConfig $config,
        private readonly LoggerInterface $logger,
    ) {}

    public function resolve(
        QueryBuilder $query,
        Connection $connection,
        TableIdentity $table,
    ): ?PrimaryKeyMetadata {
        if (array_key_exists($table->hash, $this->memo)) {
            $known = $this->memo[$table->hash]['metadata'];
            $supplied = $query->primaryKey();

            if ($supplied !== null && !$this->same($known, $supplied)) {
                $this->conflict($table, $known, $supplied);
                $this->memo[$table->hash]['metadata'] = null;

                return null;
            }

            return $known;
        }

        $candidates = [];
        $supplied = $query->primaryKey();

        if ($supplied !== null) {
            $candidates[] = $supplied;
        }

        foreach ($this->configured($table) as $configured) {
            $candidates[] = $configured;
        }

        $schema = $this->introspect($connection, $table);

        if ($schema !== null) {
            $candidates[] = $schema;
        }

        $metadata = $candidates[0] ?? null;

        foreach (array_slice($candidates, 1) as $candidate) {
            if (!$this->same($metadata, $candidate)) {
                $this->conflict($table, $metadata, $candidate);
                $metadata = null;
                break;
            }
        }

        $this->memo[$table->hash] = [
            'connection' => $table->connection,
            'metadata' => $metadata,
        ];

        return $metadata;
    }

    public function clear(?string $connection = null): void
    {
        if ($connection === null) {
            $this->memo = [];

            return;
        }

        foreach ($this->memo as $hash => $entry) {
            if ($entry['connection'] === $connection) {
                unset($this->memo[$hash]);
            }
        }
    }

    /** @return list<PrimaryKeyMetadata> */
    private function configured(TableIdentity $table): array
    {
        $matches = [];

        foreach ($this->config->primaryKeys as $override) {
            if (
                !is_array($override)
                || ($override['connection'] ?? null) !== $table->connection
                || ($override['database'] ?? null) !== $table->database
                || ($override['table'] ?? null) !== $table->table
                || array_key_exists('schema', $override)
                    && ($override['schema'] ?? null) !== $table->schema
            ) {
                continue;
            }

            $matches[] = new PrimaryKeyMetadata(
                (string) ($override['column'] ?? ''),
                (string) ($override['type'] ?? ''),
            );
        }

        return $matches;
    }

    private function same(
        ?PrimaryKeyMetadata $left,
        ?PrimaryKeyMetadata $right,
    ): bool {
        return $left === null && $right === null
            || $left !== null
                && $right !== null
                && $left->column === $right->column
                && $left->family === $right->family;
    }

    private function conflict(
        TableIdentity $table,
        ?PrimaryKeyMetadata $left,
        ?PrimaryKeyMetadata $right,
    ): void {
        if (isset($this->warnedConflicts[$table->hash])) {
            return;
        }

        $this->warnedConflicts[$table->hash] = true;
        $this->logger->warning('NormCache disabled canonical rows for a table with conflicting primary-key metadata.', [
            'table_hash' => $table->hash,
            'connection' => $table->connection,
            'table' => $table->table,
            'first' => $left === null ? null : [$left->column, $left->family],
            'second' => $right === null ? null : [$right->column, $right->family],
        ]);
    }

    private function introspect(
        Connection $connection,
        TableIdentity $table,
    ): ?PrimaryKeyMetadata {
        try {
            $schema = $connection->getSchemaBuilder();
            $indexes = $schema->getIndexes($table->qualifiedTable());
            $primaryColumns = [];

            foreach ($indexes as $index) {
                if ($index['primary'] === true || strtolower($index['name']) === 'primary') {
                    $primaryColumns = $index['columns'];
                    break;
                }
            }

            if (count($primaryColumns) !== 1) {
                return null;
            }

            $columnName = (string) $primaryColumns[0];
            $columns = $schema->getColumns($table->qualifiedTable());

            foreach ($columns as $column) {
                if ($column['name'] !== $columnName) {
                    continue;
                }

                $type = strtolower($column['type_name']);
                $family = match (true) {
                    str_contains($type, 'int') => PrimaryKeyMetadata::INTEGER,
                    str_contains($type, 'char'),
                    str_contains($type, 'text'),
                    str_contains($type, 'uuid'),
                    str_contains($type, 'ulid'),
                    str_contains($type, 'string'),
                    str_contains($type, 'binary'),
                    str_contains($type, 'blob') => PrimaryKeyMetadata::STRING,
                    default => null,
                };

                if ($family === null) {
                    return null;
                }

                return new PrimaryKeyMetadata($columnName, $family);
            }
        } catch (\Throwable) {
            return null;
        }

        return null;
    }
}
