<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Values\TableIdentity;

final class TableIdentityResolver
{
    /** @var array<string, array{connection: string, schema: ?string}> */
    private array $effectiveSchemas = [];

    /** @var array<string, ?TableIdentity> */
    private array $resolvedIdentities = [];

    public function clear(?string $connection = null): void
    {
        if ($connection === null) {
            $this->effectiveSchemas = [];
            $this->resolvedIdentities = [];

            return;
        }

        $this->effectiveSchemas = array_filter(
            $this->effectiveSchemas,
            static fn(array $entry): bool => $entry['connection'] !== $connection,
        );
        $this->resolvedIdentities = array_filter(
            $this->resolvedIdentities,
            static fn(?TableIdentity $id, string $key): bool => !str_starts_with($key, $connection . ':'),
            ARRAY_FILTER_USE_BOTH,
        );
    }

    public function resolve(Connection $connection, mixed $from): ?TableIdentity
    {
        if (!is_string($from)) {
            return null;
        }

        $cacheKey = $this->mutableConnectionKey($connection) . ':' . $from;

        if (array_key_exists($cacheKey, $this->resolvedIdentities)) {
            return $this->resolvedIdentities[$cacheKey];
        }

        $identity = $this->doResolve($connection, $from);

        if ($identity !== null) {
            $this->resolvedIdentities[$cacheKey] = $identity;
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
        $key = $this->mutableConnectionKey($connection) . ':schema';

        if (array_key_exists($key, $this->effectiveSchemas)) {
            return $this->effectiveSchemas[$key]['schema'];
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

        if ($schema !== null) {
            $this->effectiveSchemas[$key] = [
                'connection' => (string) $connection->getName(),
                'schema' => $schema,
            ];
        }

        return $schema;
    }

    private function isView(Connection $connection, string $schema, string $table): ?bool
    {
        try {
            $view = $this->unqualifiedTable($table);
            $reference = $schema === '' ? $view : $schema . '.' . $view;

            return $connection->getSchemaBuilder()->hasView($reference);
        } catch (\Throwable) {
            return null;
        }
    }

    private function mutableConnectionKey(Connection $connection): string
    {
        $name = (string) $connection->getName();
        $configFingerprint = hash('xxh128', serialize([
            $connection->getConfig('search_path'),
            $connection->getConfig('schema'),
            $connection->getConfig('username'),
        ]));

        return $name . ':' . spl_object_id($connection) . ':' . hash(
            'xxh128',
            TableIdentity::encodeFields([
                (string) $connection->getDriverName(),
                (string) $connection->getDatabaseName(),
                (string) $connection->getTablePrefix(),
                $configFingerprint,
            ]),
        );
    }

    private function unqualifiedTable(string $table): string
    {
        $position = strrpos($table, '.');

        return trim($position === false ? $table : substr($table, $position + 1), '`"[]');
    }
}
