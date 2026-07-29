<?php

namespace NormCache\Planning;

use NormCache\Values\TableIdentity;

final class ConnectionMetadata
{
    public ?string $schema = null;

    /** @var array<string, TableIdentity> */
    public array $identities = [];

    /** @var array<string, array<string, true>> */
    public array $views = [];
}
