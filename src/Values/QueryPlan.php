<?php

namespace NormCache\Values;

final readonly class QueryPlan
{
    public const CANONICAL = 'canonical';

    public const EXACT = 'exact';

    public const QUERY_GROUP = 'query-group';

    public const DIRECT_PK = 'direct-pk';

    /** @param list<TableIdentity> $dependencies */
    public function __construct(
        public string $route,
        public TableIdentity $root,
        public array $dependencies,
        public ?PrimaryKeyMetadata $primaryKey = null,
        public ?string $primaryKeyToken = null,
        public ?string $softDeleteMode = null,
        public ?string $deletedAtColumn = null,
    ) {}
}
