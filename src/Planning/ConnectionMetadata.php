<?php

namespace NormCache\Planning;

use NormCache\Values\TableIdentity;

final class ConnectionMetadata
{
    public ?string $schema = null;

    public bool $schemaResolved = false;

    /** @var array<string, TableIdentity> */
    public array $identities = [];

    /** @var array<string, array<string, true>> */
    public array $views = [];

    /** @var array<string, string|null> */
    public array $attachments = [];

    public function __construct(
        public readonly string $sourceScope,
        public readonly string $database,
        public readonly string $prefix,
    ) {}
}
