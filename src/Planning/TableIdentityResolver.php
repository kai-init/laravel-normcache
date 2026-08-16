<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Values\TableIdentity;

final class TableIdentityResolver
{
    private const UNRESOLVABLE_LIMIT = 256;

    /**
     * @var \WeakMap<Connection, array{
     *     signature: string,
     *     identities: array<string, TableIdentity>,
     *     unresolvable: array<string, true>
     * }>
     */
    private \WeakMap $connections;

    public function __construct()
    {
        $this->connections = new \WeakMap;
    }

    public function resolve(Connection $connection, mixed $from): ?TableIdentity
    {
        if (!is_string($from)) {
            return null;
        }

        $sourceScope = ConnectionSourceResolver::resolve($connection);

        if ($sourceScope === null) {
            return null;
        }

        $metadata = $this->metadata($connection, $sourceScope);

        if (isset($metadata['identities'][$from])) {
            return $metadata['identities'][$from];
        }

        if (isset($metadata['unresolvable'][$from])) {
            return null;
        }

        $identity = $this->resolveIdentity($connection, $from, $sourceScope);

        if ($identity === null) {
            // Raw expressions and subquery SQL land here, so unlike the identity map
            // this one is not bounded by the number of real tables.
            if (count($metadata['unresolvable']) >= self::UNRESOLVABLE_LIMIT) {
                $metadata['unresolvable'] = [];
            }

            $metadata['unresolvable'][$from] = true;
        } else {
            $metadata['identities'][$from] = $identity;
        }

        $this->connections[$connection] = $metadata;

        return $identity;
    }

    private function resolveIdentity(
        Connection $connection,
        string $from,
        string $sourceScope,
    ): ?TableIdentity {
        $table = $this->physicalTable($from);

        if ($table === null) {
            return null;
        }

        $driver = (string) $connection->getDriverName();
        $database = (string) $connection->getDatabaseName();
        $parts = array_map($this->unquote(...), explode('.', $table));
        $maximumParts = match ($driver) {
            'mysql', 'mariadb', 'pgsql', 'sqlite' => 2,
            'sqlsrv' => 3,
            default => 1,
        };

        if (
            in_array('', $parts, true)
            || count($parts) > $maximumParts
        ) {
            return null;
        }

        $resolvedTable = $parts[count($parts) - 1];
        $schema = '';

        if (in_array($driver, ['mysql', 'mariadb'], true)) {
            $database = count($parts) === 2 ? $parts[0] : $database;
            $schema = $database;
        } elseif ($driver === 'pgsql') {
            $schema = count($parts) === 2
                ? $parts[0]
                : $this->configuredSchema($connection, 'public');
        } elseif ($driver === 'sqlsrv') {
            if (count($parts) === 3) {
                $database = $parts[0];
                $schema = $parts[1];
            } else {
                $schema = count($parts) === 2
                    ? $parts[0]
                    : $this->configuredSchema($connection, 'dbo');
            }
        } elseif ($driver === 'sqlite') {
            $schema = strtolower(count($parts) === 2 ? $parts[0] : 'main');
            $resolvedTable = strtolower($resolvedTable);
            $database = $this->sqliteDatabase($database);

            if ($database === '') {
                return null;
            }
        }

        return TableIdentity::fromParts(
            driver: $driver,
            connection: (string) $connection->getName(),
            database: $database,
            schema: $schema,
            prefix: (string) $connection->getTablePrefix(),
            table: $resolvedTable,
            sourceScope: $sourceScope,
        );
    }

    private function physicalTable(string $from): ?string
    {
        $from = trim($from);

        if (preg_match('/(?:`[^`]*\s[^`]*`|"[^"]*\s[^"]*"|\[[^]]*\s[^]]*\])/u', $from) === 1) {
            return null;
        }

        if (preg_match('/^([^\s]+)(?:\s+(?:as\s+)?[^\s]+)?$/i', $from, $matches) !== 1) {
            return null;
        }

        $table = $matches[1];

        return preg_match('/[(){}:,*]/', $table) === 0 ? $table : null;
    }

    private function unquote(string $identifier): string
    {
        return trim($identifier, '`"[]');
    }

    private function configuredSchema(Connection $connection, string $default): string
    {
        $config = $connection->getConfig();
        $configured = $config['search_path'] ?? $config['schema'] ?? $default;
        $candidates = is_array($configured)
            ? $configured
            : explode(',', (string) $configured);

        foreach ($candidates as $candidate) {
            $schema = trim((string) $candidate, " \t\n\r\0\x0B`\"'");

            // PostgreSQL resolves $user to the session user.
            if ($schema === '$user') {
                $schema = trim((string) ($config['username'] ?? ''));
            }

            if ($schema !== '') {
                return $schema;
            }
        }

        return $default;
    }

    private function sqliteDatabase(string $database): string
    {
        if ($database === '' || str_contains($database, ':memory:')) {
            return '';
        }

        return realpath($database) ?: $database;
    }

    /**
     * @return array{
     *     signature: string,
     *     identities: array<string, TableIdentity>,
     *     unresolvable: array<string, true>
     * }
     */
    private function metadata(Connection $connection, string $sourceScope): array
    {
        $signature = TableIdentity::encodeFields([
            $sourceScope,
            (string) $connection->getDriverName(),
            (string) $connection->getDatabaseName(),
            (string) $connection->getTablePrefix(),
        ]);
        $metadata = $this->connections[$connection] ?? null;

        if ($metadata === null || $metadata['signature'] !== $signature) {
            $metadata = ['signature' => $signature, 'identities' => [], 'unresolvable' => []];
            $this->connections[$connection] = $metadata;
        }

        return $metadata;
    }
}
