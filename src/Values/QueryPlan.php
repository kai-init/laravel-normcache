<?php

namespace NormCache\Values;

final readonly class QueryPlan
{
    public const CANONICAL = 'canonical';

    public const RESULT = 'result';

    public const QUERY_GROUP = 'query-group';

    public const DIRECT_PK = 'direct-pk';

    /**
     * @param  list<TableIdentity>  $dependencies
     * @param  list<string>|null  $projectedColumns
     */
    private function __construct(
        public string $route,
        public TableIdentity $root,
        public array $dependencies,
        public ?PrimaryKeyMetadata $primaryKey = null,
        public ?string $primaryKeyToken = null,
        public ?string $softDeleteMode = null,
        public ?string $deletedAtColumn = null,
        public ?array $projectedColumns = null,
    ) {}

    /** @param list<TableIdentity> $dependencies */
    public static function queryGroup(TableIdentity $root, array $dependencies): self
    {
        return new self(self::QUERY_GROUP, $root, $dependencies);
    }

    /** @param list<TableIdentity> $dependencies */
    public static function directPrimaryKey(
        TableIdentity $root,
        array $dependencies,
        PrimaryKeyMetadata $primaryKey,
        string $primaryKeyToken,
        ?string $softDeleteMode,
        ?string $deletedAtColumn,
    ): self {
        return new self(
            self::DIRECT_PK,
            $root,
            $dependencies,
            $primaryKey,
            $primaryKeyToken,
            $softDeleteMode,
            $deletedAtColumn,
        );
    }

    /** @param list<TableIdentity> $dependencies */
    public static function canonical(
        TableIdentity $root,
        array $dependencies,
        PrimaryKeyMetadata $primaryKey,
    ): self {
        return new self(
            self::CANONICAL,
            $root,
            $dependencies,
            $primaryKey,
        );
    }

    /** @param list<TableIdentity> $dependencies */
    public static function result(
        TableIdentity $root,
        array $dependencies,
        ?PrimaryKeyMetadata $primaryKey = null,
    ): self {
        return new self(self::RESULT, $root, $dependencies, $primaryKey);
    }

    /**
     * @param  list<TableIdentity>  $dependencies
     * @param  list<string>  $projectedColumns
     */
    public static function projectedResult(
        TableIdentity $root,
        array $dependencies,
        PrimaryKeyMetadata $primaryKey,
        array $projectedColumns,
    ): self {
        return new self(
            self::RESULT,
            $root,
            $dependencies,
            $primaryKey,
            projectedColumns: $projectedColumns,
        );
    }

    /**
     * @param  list<TableIdentity>  $dependencies
     * @param  list<string>  $projectedColumns
     */
    public static function projectedRow(
        TableIdentity $root,
        array $dependencies,
        PrimaryKeyMetadata $primaryKey,
        string $primaryKeyToken,
        array $projectedColumns,
        ?string $softDeleteMode,
        ?string $deletedAtColumn,
    ): self {
        return new self(
            self::RESULT,
            $root,
            $dependencies,
            $primaryKey,
            $primaryKeyToken,
            $softDeleteMode,
            $deletedAtColumn,
            $projectedColumns,
        );
    }

    public function isCanonical(): bool
    {
        return $this->route === self::CANONICAL;
    }

    public function isResult(): bool
    {
        return $this->route === self::RESULT;
    }

    public function isQueryGroup(): bool
    {
        return $this->route === self::QUERY_GROUP;
    }

    public function isDirectPrimaryKey(): bool
    {
        return $this->route === self::DIRECT_PK;
    }

    public function usesGeneration(): bool
    {
        return $this->isCanonical() || $this->isDirectPrimaryKey();
    }

    public function supportsRowFallback(): bool
    {
        return $this->isResult()
            && $this->primaryKey !== null
            && $this->projectedColumns !== null
            && $this->primaryKeyToken !== null;
    }

    public function supportsCanonicalProjectionFallback(): bool
    {
        return $this->isResult()
            && $this->primaryKey !== null
            && $this->projectedColumns !== null
            && $this->primaryKeyToken === null;
    }
}
