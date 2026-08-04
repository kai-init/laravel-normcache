<?php

namespace NormCache\Facades;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Facade;

/**
 * @method static bool invalidate(Model|string|array $targets, ?string $connection = null)
 * @method static bool invalidateTable(string $connection, string $table)
 * @method static bool invalidateTables(string $connection, array $tables)
 * @method static bool flushTag(string $tag)
 * @method static bool flushAll()
 * @method static bool disableCache()
 * @method static int|null enableCache()
 * @method static bool cacheDisabled()
 * @method static bool clearSchema()
 * @method static bool refreshSchema(?string $connection = null)
 */
final class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
