<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;

final class SchemaRepository
{
    private const NONE = '-';

    private ?string $epoch = null;

    /** @var array<string, string> */
    private array $connectionEpochs = [];

    /** @var array<string, array<string, ?string>> */
    private array $fields = [];

    /** @var array<string, array<string, list<array{table: string, action: string}>>> */
    private array $deleteActionGraphs = [];

    public function __construct(
        private readonly CacheConfig $config,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
    ) {}

    public function prime(Connection $connection, string $schema, TableIdentity $table): void
    {
        if ($this->config->schemaTtl === 0) {
            return;
        }

        try {
            $key = $this->metadataKey((string) $connection->getName());
            $requested = [
                $this->viewsField($connection, $schema),
                'primary-key:' . $table->hash,
            ];
            $missing = array_values(array_filter(
                $requested,
                fn(string $field): bool => !array_key_exists($field, $this->fields[$key] ?? []),
            ));

            if ($missing === []) {
                return;
            }

            $values = $this->store->readSchemaFields($key, $missing);

            foreach ($missing as $index => $field) {
                $this->fields[$key][$field] = $values[$index] ?? null;
            }
        } catch (\Throwable) {
            // Individual metadata reads and database introspection remain available.
        }
    }

    /** @return array<string, true>|null */
    public function views(Connection $connection, string $schema): ?array
    {
        $raw = $this->read($connection, $this->viewsField($connection, $schema));

        if ($raw === null) {
            return null;
        }

        try {
            $names = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);

            if (!is_array($names)) {
                return null;
            }

            $views = [];

            foreach ($names as $name) {
                if (!is_string($name) || $name === '') {
                    return null;
                }

                $views[$name] = true;
            }

            return $views;
        } catch (\Throwable) {
            return null;
        }
    }

    /** @param array<string, true> $views */
    public function putViews(Connection $connection, string $schema, array $views): void
    {
        $names = array_keys($views);
        sort($names);

        try {
            $this->write(
                $connection,
                $this->viewsField($connection, $schema),
                json_encode($names, JSON_THROW_ON_ERROR),
            );
        } catch (\Throwable) {
            // Schema introspection remains the fail-open source of truth.
        }
    }

    public function effectiveSchema(Connection $connection): string|false
    {
        $raw = $this->read($connection, $this->schemaField($connection));

        return is_string($raw) && $raw !== '' ? $raw : false;
    }

    public function putEffectiveSchema(Connection $connection, string $schema): void
    {
        if ($schema !== '') {
            $this->write($connection, $this->schemaField($connection), $schema);
        }
    }

    public function primaryKey(TableIdentity $table): PrimaryKeyMetadata|null|false
    {
        $raw = $this->readForConnection($table->connection, 'primary-key:' . $table->hash);

        if ($raw === null) {
            return false;
        }

        if ($raw === self::NONE) {
            return null;
        }

        try {
            $metadata = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
            $column = is_array($metadata) ? ($metadata['column'] ?? null) : null;
            $family = is_array($metadata) ? ($metadata['family'] ?? null) : null;

            if (!is_string($column) || !is_string($family)) {
                return false;
            }

            return new PrimaryKeyMetadata($column, $family);
        } catch (\Throwable) {
            return false;
        }
    }

    public function putPrimaryKey(TableIdentity $table, ?PrimaryKeyMetadata $metadata): void
    {
        try {
            $value = $metadata === null
                ? self::NONE
                : json_encode([
                    'column' => $metadata->column,
                    'family' => $metadata->family,
                ], JSON_THROW_ON_ERROR);

            $this->writeForConnection(
                $table->connection,
                'primary-key:' . $table->hash,
                $value,
            );
        } catch (\Throwable) {
            // Schema introspection remains the fail-open source of truth.
        }
    }

    /** @return array<string, list<array{table: string, action: string}>>|null */
    public function deleteActions(Connection $connection, bool $fresh = false): ?array
    {
        $field = $this->deleteActionsField($connection);

        if ($fresh) {
            unset($this->deleteActionGraphs[$field]);

            try {
                $key = $this->metadataKey((string) $connection->getName());
                unset($this->fields[$key][$field]);
            } catch (\Throwable) {
                return null;
            }
        }

        if (array_key_exists($field, $this->deleteActionGraphs)) {
            return $this->deleteActionGraphs[$field];
        }

        $raw = $this->read($connection, $field);

        if ($raw === null) {
            return null;
        }

        try {
            $decoded = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);

            if (!is_array($decoded)) {
                return null;
            }

            $graph = [];

            foreach ($decoded as $parent => $edges) {
                if (!is_string($parent) || $parent === '' || !is_array($edges)) {
                    return null;
                }

                foreach ($edges as $edge) {
                    $table = is_array($edge) ? ($edge['table'] ?? null) : null;
                    $action = is_array($edge) ? ($edge['action'] ?? null) : null;

                    if (
                        !is_string($table)
                        || $table === ''
                        || !in_array($action, ['cascade', 'set null', 'set default'], true)
                    ) {
                        return null;
                    }

                    $graph[$parent][] = ['table' => $table, 'action' => $action];
                }
            }

            return $this->deleteActionGraphs[$field] = $graph;
        } catch (\Throwable) {
            return null;
        }
    }

    /** @param array<string, list<array{table: string, action: string}>> $graph */
    public function putDeleteActions(Connection $connection, array $graph): void
    {
        try {
            $field = $this->deleteActionsField($connection);
            $value = json_encode($graph, JSON_THROW_ON_ERROR);
            $this->deleteActionGraphs[$field] = $graph;
            $this->write(
                $connection,
                $field,
                $value,
            );
        } catch (\Throwable) {
            // Schema introspection remains the fail-open source of truth.
        }
    }

    public function deleteActionsBuildKey(Connection $connection): string
    {
        return $this->metadataKey((string) $connection->getName())
            . ':build:' . hash('xxh128', $this->connectionScope($connection));
    }

    public function clear(?string $connection = null): bool
    {
        if ($this->config->schemaTtl === 0) {
            $this->fields = [];
            $this->deleteActionGraphs = [];

            return true;
        }

        try {
            if ($connection !== null) {
                $this->connectionEpochs[$connection] = (string) $this->store->increment(
                    $this->keys->connectionSchemaEpoch($connection),
                );

                return true;
            }

            $this->epoch = (string) $this->store->increment(
                $this->keys->schemaEpoch(),
            );

            return true;
        } catch (\Throwable) {
            if ($connection === null) {
                $this->epoch = null;
            } else {
                unset($this->connectionEpochs[$connection]);
            }

            return false;
        } finally {
            $this->fields = [];
            $this->deleteActionGraphs = [];
        }
    }

    private function read(Connection $connection, string $field): ?string
    {
        return $this->readForConnection((string) $connection->getName(), $field);
    }

    private function readForConnection(string $connection, string $field): ?string
    {
        if ($this->config->schemaTtl === 0) {
            return null;
        }

        try {
            $key = $this->metadataKey($connection);

            if (array_key_exists($field, $this->fields[$key] ?? [])) {
                return $this->fields[$key][$field];
            }

            return $this->fields[$key][$field] = $this->store->readSchemaField($key, $field);
        } catch (\Throwable) {
            return null;
        }
    }

    private function write(Connection $connection, string $field, string $value): void
    {
        $this->writeForConnection((string) $connection->getName(), $field, $value);
    }

    private function writeForConnection(string $connection, string $field, string $value): void
    {
        if ($this->config->schemaTtl === 0) {
            return;
        }

        try {
            $key = $this->metadataKey($connection);
            $this->store->writeSchemaField(
                $key,
                $field,
                $value,
                $this->config->schemaTtl,
            );
            $this->fields[$key][$field] = $value;
        } catch (\Throwable) {
            // Schema introspection remains the fail-open source of truth.
        }
    }

    private function metadataKey(string $connection): string
    {
        $this->resolveEpochs($connection);

        return $this->keys->schema(
            $connection,
            (string) $this->epoch,
            $this->connectionEpochs[$connection],
        );
    }

    private function resolveEpochs(string $connection): void
    {
        if ($this->epoch !== null && array_key_exists($connection, $this->connectionEpochs)) {
            return;
        }

        $epochKey = $this->keys->schemaEpoch();
        $connectionKey = $this->keys->connectionSchemaEpoch($connection);
        $keys = [];

        if ($this->epoch === null) {
            $keys[] = $epochKey;
        }

        if (!array_key_exists($connection, $this->connectionEpochs)) {
            $keys[] = $connectionKey;
        }

        $values = $this->store->mget($keys);
        $this->epoch ??= $values[$epochKey] ?? '0';
        $this->connectionEpochs[$connection] ??= $values[$connectionKey] ?? '0';
    }

    private function schemaField(Connection $connection): string
    {
        return 'effective-schema:' . $this->connectionScope($connection);
    }

    private function viewsField(Connection $connection, string $schema): string
    {
        return 'views:' . hash('xxh128', $this->connectionScope($connection) . "\0" . $schema);
    }

    private function deleteActionsField(Connection $connection): string
    {
        return 'delete-actions:' . hash('xxh128', $this->connectionScope($connection));
    }

    private function connectionScope(Connection $connection): string
    {
        return implode("\0", [
            ConnectionSourceResolver::resolve($connection),
            (string) $connection->getDriverName(),
            (string) $connection->getDatabaseName(),
            (string) $connection->getTablePrefix(),
        ]);
    }
}
