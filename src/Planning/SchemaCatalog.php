<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;
use NormCache\Database\QueryBuilder;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;

final readonly class SchemaCatalog
{
    public function __construct(
        private SchemaRepository $persistent,
        private TableIdentityResolver $tables,
        private PrimaryKeyResolver $primaryKeys,
        private CascadeDependencyResolver $cascades,
    ) {}

    public function resolveTable(Connection $connection, mixed $source): ?TableIdentity
    {
        return $this->tables->resolve($connection, $source);
    }

    public function resolveBaseTable(Connection $connection, mixed $source): ?TableIdentity
    {
        return $this->tables->resolveBaseTable($connection, $source);
    }

    public function resolvePrimaryKey(
        QueryBuilder $query,
        Connection $connection,
        TableIdentity $table,
    ): ?PrimaryKeyMetadata {
        return $this->primaryKeys->resolve($query, $connection, $table);
    }

    /** @return list<TableIdentity>|null */
    public function affectedByDelete(
        Connection $connection,
        TableIdentity $table,
    ): ?array {
        return $this->cascades->affectedByDelete($connection, $table);
    }

    public function warm(Connection $connection): void
    {
        $this->cascades->warm($connection);
    }

    public function clear(): bool
    {
        $this->tables->clear();
        $this->primaryKeys->clear();

        return $this->persistent->clear();
    }
}
