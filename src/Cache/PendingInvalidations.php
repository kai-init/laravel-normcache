<?php

namespace NormCache\Cache;

use Illuminate\Database\Connection;
use NormCache\Values\TableIdentity;

final class PendingInvalidations
{
    /** @var array<string, array<string, array{table: TableIdentity, broad: bool, tokens: array<string, true>}>> */
    public array $tables = [];

    /** @var array<string, string> */
    public array $global = [];

    /** @var array<string, Connection> */
    public array $connections = [];

    /** @var array<string, true> */
    public array $commitAttempts = [];
}
