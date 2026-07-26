<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static bool invalidateTable(string $connection, string $table)
 * @method static bool invalidateTables(string $connection, array $tables)
 * @method static bool flushTag(string $tag)
 * @method static bool flushAll()
 * @method static void clearSchemaMetadata(?string $connection = null)
 */
final class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
