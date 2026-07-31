<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Values\TableIdentity;

final class TableIdentityResolver
{
    /** @var \WeakMap<Connection, ConnectionMetadata> */
    private \WeakMap $connections;

    public function __construct()
    {
        $this->connections = new \WeakMap;
    }

    public function clear(?string $connection = null): void
    {
        if ($connection === null) {
            $this->connections = new \WeakMap;

            return;
        }

        foreach ($this->connections as $bound => $_) {
            if ((string) $bound->getName() === $connection) {
                unset($this->connections[$bound]);
            }
        }
    }

    public function resolve(Connection $connection, mixed $from): ?TableIdentity
    {
        if (!is_string($from)) {
            return null;
        }

        $metadata = $this->metadata($connection);

        if (array_key_exists($from, $metadata->identities)) {
            return $metadata->identities[$from];
        }

        $identity = $this->doResolve($connection, $from);

        if ($identity !== null) {
            $metadata->identities[$from] = $identity;
        }

        return $identity;
    }

    private function doResolve(Connection $connection, string $from): ?TableIdentity
    {
        $table = $this->physicalTable($from);

        if ($table === null) {
            return null;
        }

        $driver = (string) $connection->getDriverName();
        $connectionName = (string) $connection->getName();
        $database = (string) $connection->getDatabaseName();
        $parts = array_map(
            static fn(string $part): string => trim($part, '`"[]'),
            explode('.', $table),
        );

        if (
            in_array($driver, ['mysql', 'mariadb'], true)
            && count($parts) === 2
        ) {
            $database = $parts[0];
        } elseif ($driver === 'sqlsrv' && count($parts) === 3) {
            $database = $parts[0];
        }

        $schema = $this->schema($connection, $driver, $database, $table);

        if ($schema === null) {
            return null;
        }

        $resolvedTable = $this->unqualifiedTable($table);

        if ($driver === 'sqlite') {
            $schema = strtolower($schema === '' ? 'main' : $schema);
            $resolvedTable = strtolower($resolvedTable);

            if ($schema !== 'main') {
                $database = (string) $this->sqliteAttachmentPath($connection, $schema);
            }

            if ($database === ':memory:' || $database === '') {
                return null;
            }

            $real = realpath($database);

            if ($real === false) {
                return null;
            }

            $database = $real;
        }

        $isView = $this->isView($connection, $schema, $resolvedTable);

        if ($isView === null) {
            return null;
        }

        return TableIdentity::fromParts(
            driver: $driver,
            connection: $connectionName,
            database: $database,
            schema: $schema,
            prefix: (string) $connection->getTablePrefix(),
            table: $resolvedTable,
            isView: $isView,
        );
    }

    private function physicalTable(string $from): ?string
    {
        $from = trim($from);

        if (preg_match('/^([^\s]+)(?:\s+(?:as\s+)?[^\s]+)?$/i', $from, $matches) !== 1) {
            return null;
        }

        $table = trim($matches[1], '`"[]');

        return $table !== '' ? $table : null;
    }

    private function schema(
        Connection $connection,
        string $driver,
        string $database,
        string $table,
    ): ?string {
        if ($driver === 'mysql' || $driver === 'mariadb') {
            return $database;
        }

        if (str_contains($table, '.')) {
            $parts = array_map(
                static fn(string $part): string => trim($part, '`"[]'),
                explode('.', $table),
            );

            return $driver === 'sqlsrv'
                ? $parts[count($parts) - 2]
                : $parts[0];
        }

        return match ($driver) {
            'pgsql', 'sqlsrv' => $this->effectiveSchema($connection),
            default => '',
        };
    }

    private function effectiveSchema(Connection $connection): ?string
    {
        $metadata = $this->metadata($connection);

        if ($metadata->schema !== null) {
            return $metadata->schema;
        }

        try {
            $schema = $connection->getSchemaBuilder()->getCurrentSchemaName();
            $schema = is_string($schema) ? trim($schema) : null;

            if ($schema === '') {
                $schema = null;
            }
        } catch (\Throwable) {
            $schema = null;
        }

        return $metadata->schema = $schema;
    }

    private function sqliteAttachmentPath(Connection $connection, string $schema): ?string
    {
        $metadata = $this->metadata($connection);

        if (array_key_exists($schema, $metadata->attachments)) {
            return $metadata->attachments[$schema];
        }

        try {
            $path = null;

            foreach ($connection->select('pragma database_list') as $attachment) {
                if (strtolower((string) $attachment->name) === $schema) {
                    $path = ((string) $attachment->file) ?: null;

                    break;
                }
            }

            return $metadata->attachments[$schema] = $path;
        } catch (\Throwable) {
            return null;
        }
    }

    private function isView(Connection $connection, string $schema, string $table): ?bool
    {
        $metadata = $this->metadata($connection);
        $name = strtolower((string) $connection->getTablePrefix() . $this->unqualifiedTable($table));

        if (isset($metadata->views[$schema])) {
            return isset($metadata->views[$schema][$name]);
        }

        try {
            $views = [];

            $viewSchema = $schema === '' && $connection->getDriverName() === 'sqlite'
                ? 'main'
                : ($schema === '' ? null : $schema);

            foreach ($connection->getSchemaBuilder()->getViews($viewSchema) as $view) {
                $views[strtolower($view['name'])] = true;
            }

            $metadata->views[$schema] = $views;

            return isset($views[$name]);
        } catch (\Throwable) {
            return null;
        }
    }

    private function metadata(Connection $connection): ConnectionMetadata
    {
        $database = (string) $connection->getDatabaseName();
        $prefix = (string) $connection->getTablePrefix();
        $metadata = $this->connections[$connection] ?? null;

        if (
            $metadata === null
            || $metadata->database !== $database
            || $metadata->prefix !== $prefix
        ) {
            $metadata = new ConnectionMetadata($database, $prefix);
            $this->connections[$connection] = $metadata;
        }

        return $metadata;
    }

    private function unqualifiedTable(string $table): string
    {
        $position = strrpos($table, '.');

        return trim($position === false ? $table : substr($table, $position + 1), '`"[]');
    }
}
