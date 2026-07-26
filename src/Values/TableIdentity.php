<?php

namespace NormCache\Values;

final readonly class TableIdentity
{
    public function __construct(
        public string $driver,
        public string $deployment,
        public string $connection,
        public string $database,
        public string $schema,
        public string $prefix,
        public string $table,
        public string $encoded,
        public string $hash,
    ) {}

    public static function fromParts(
        string $driver,
        string $deployment,
        string $connection,
        string $database,
        string $schema,
        string $prefix,
        string $table,
    ): self {
        $encoded = self::encodeFields([
            'nc4-table-v1',
            $driver,
            $deployment,
            $connection,
            $database,
            $schema,
            $prefix,
            $table,
        ]);

        return new self(
            driver: $driver,
            deployment: $deployment,
            connection: $connection,
            database: $database,
            schema: $schema,
            prefix: $prefix,
            table: $table,
            encoded: $encoded,
            hash: hash('xxh128', $encoded),
        );
    }

    public function qualifiedTable(): string
    {
        return match ($this->driver) {
            'mysql', 'mariadb' => $this->database . '.' . $this->table,
            'pgsql' => $this->schema . '.' . $this->table,
            'sqlsrv' => $this->schema . '.' . $this->table,
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
