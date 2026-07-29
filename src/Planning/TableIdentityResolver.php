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

        if ($driver === 'sqlite') {
            if ($database === ':memory:' || $database === '') {
                return null;
            }

            $real = realpath($database);

            if ($real === false) {
                return null;
            }

            $database = $real;
        }

        $schema = $this->schema($connection, $driver, $database, $table);

        if ($schema === null) {
            return null;
        }

        $isView = $this->isView($connection, $schema, $table);

        if ($isView === null) {
            return null;
        }

        return TableIdentity::fromParts(
            driver: $driver,
            connection: $connectionName,
            database: $database,
            schema: $schema,
            prefix: (string) $connection->getTablePrefix(),
            table: $this->unqualifiedTable($table),
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
        return $this->connections[$connection] ??= new ConnectionMetadata;
    }

    private function unqualifiedTable(string $table): string
    {
        $position = strrpos($table, '.');

        return trim($position === false ? $table : substr($table, $position + 1), '`"[]');
    }
}
