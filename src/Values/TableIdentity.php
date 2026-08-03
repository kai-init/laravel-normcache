<?php

namespace NormCache\Values;

final readonly class TableIdentity
{
    private const FORMAT = 'nc-table';

    public function __construct(
        public string $driver,
        public string $connection,
        public string $sourceScope,
        public string $database,
        public string $schema,
        public string $prefix,
        public string $table,
        public string $encoded,
        public string $hash,
        public bool $isView = false,
    ) {}

    public static function fromParts(
        string $driver,
        string $connection,
        string $database,
        string $schema,
        string $prefix,
        string $table,
        bool $isView = false,
        ?string $sourceScope = null,
    ): self {
        $sourceScope ??= $connection;

        if ($sourceScope === '') {
            throw new \InvalidArgumentException('NormCache table source scope must be non-empty.');
        }

        $encoded = self::encodeFields([
            self::FORMAT,
            $sourceScope,
            $driver,
            $database,
            $schema,
            $prefix,
            $table,
        ]);

        return new self(
            driver: $driver,
            connection: $connection,
            sourceScope: $sourceScope,
            database: $database,
            schema: $schema,
            prefix: $prefix,
            table: $table,
            encoded: $encoded,
            hash: hash('xxh128', $encoded),
            isView: $isView,
        );
    }

    public function asView(): self
    {
        return new self(
            driver: $this->driver,
            connection: $this->connection,
            sourceScope: $this->sourceScope,
            database: $this->database,
            schema: $this->schema,
            prefix: $this->prefix,
            table: $this->table,
            encoded: $this->encoded,
            hash: $this->hash,
            isView: true,
        );
    }

    public function qualifiedTable(): string
    {
        return match ($this->driver) {
            'mysql', 'mariadb' => $this->database . '.' . $this->table,
            'pgsql' => $this->schema . '.' . $this->table,
            'sqlsrv' => $this->database . '.' . $this->schema . '.' . $this->table,
            'sqlite' => $this->schema === ''
                ? $this->table
                : $this->schema . '.' . $this->table,
            default => $this->table,
        };
    }

    /** @param list<string> $fields */
    public static function encodeFields(array $fields): string
    {
        $encoded = '';

        foreach ($fields as $field) {
            $encoded .= strlen($field) . ':' . $field;
        }

        return $encoded;
    }
}
