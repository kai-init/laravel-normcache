<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;
use NormCache\CacheManager;

/**
 * @mixin CacheManager
 *
 * @method static \NormCache\Cache\ExecutionEngine engine()
 * @method static \NormCache\Cache\ResultExecutor result()
 * @method static \NormCache\Cache\ModelHydrator hydrator()
 * @method static \NormCache\Cache\NormalizedCacheRepository queries()
 * @method static \NormCache\Cache\ResultCacheRepository results()
 * @method static \NormCache\Cache\ThroughCacheRepository through()
 * @method static \NormCache\Cache\ModelCacheRepository models()
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
