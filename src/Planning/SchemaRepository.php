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

    public function __construct(
        private readonly CacheConfig $config,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
    ) {}

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

    public function clear(?string $connection = null): bool
    {
        if ($this->config->schemaTtl === 0) {
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
            return $this->store->readSchemaField($this->metadataKey($connection), $field);
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
            $this->store->writeSchemaField(
                $this->metadataKey($connection),
                $field,
                $value,
                $this->config->schemaTtl,
            );
        } catch (\Throwable) {
            // Schema introspection remains the fail-open source of truth.
        }
    }

    private function metadataKey(string $connection): string
    {
        return $this->keys->schema(
            $connection,
            $this->currentEpoch(),
            $this->currentConnectionEpoch($connection),
        );
    }

    private function currentEpoch(): string
    {
        if ($this->epoch !== null) {
            return $this->epoch;
        }

        return $this->epoch = $this->store->getRaw(
            $this->keys->schemaEpoch(),
        ) ?? '0';
    }

    private function currentConnectionEpoch(string $connection): string
    {
        return $this->connectionEpochs[$connection] ??= $this->store->getRaw(
            $this->keys->connectionSchemaEpoch($connection),
        ) ?? '0';
    }

    private function schemaField(Connection $connection): string
    {
        return 'effective-schema:' . $this->connectionScope($connection);
    }

    private function viewsField(Connection $connection, string $schema): string
    {
        return 'views:' . hash('xxh128', $this->connectionScope($connection) . "\0" . $schema);
    }

    private function connectionScope(Connection $connection): string
    {
        return implode("\0", [
            (string) $connection->getDriverName(),
            (string) $connection->getDatabaseName(),
            (string) $connection->getTablePrefix(),
        ]);
    }
}
