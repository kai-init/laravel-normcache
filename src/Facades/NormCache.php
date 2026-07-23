<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;
use NormCache\CacheManager;

/**
 * @mixin CacheManager
 *
 * @method static \NormCache\Cache\ModelIndexCache modelIndexes()
 * @method static \NormCache\Cache\ResultCache resultCache()
 * @method static \NormCache\Cache\RelationIndexCache relationIndexes()
 * @method static \NormCache\Cache\ModelCache modelCache()
 * @method static \NormCache\Cache\VersionStore versionStore()
 * @method static \NormCache\Cache\Invalidator invalidator()
 * @method static \NormCache\Support\RedisStore store()
 * @method static \NormCache\Support\CacheKeyBuilder keys()
 * @method static \NormCache\Values\CacheConfig config()
 */
class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
